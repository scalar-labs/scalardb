package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class ObjectStorageRecordReadSupport extends ReadSupport<ObjectStorageRecord> {
  private final TableMetadata tableMetadata;

  public ObjectStorageRecordReadSupport(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  @Override
  public ReadContext init(InitContext context) {
    return new ReadContext(context.getFileSchema());
  }

  @Override
  public RecordMaterializer<ObjectStorageRecord> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    return new ObjectStorageRecordMaterializer(fileSchema, tableMetadata);
  }

  static RecordMaterializer<ObjectStorageRecord> createMaterializer(
      MessageType schema, TableMetadata tableMetadata) {
    return new ObjectStorageRecordMaterializer(schema, tableMetadata);
  }

  private static class ObjectStorageRecordMaterializer
      extends RecordMaterializer<ObjectStorageRecord> {
    private final ObjectStorageRecordGroupConverter root;

    ObjectStorageRecordMaterializer(MessageType schema, TableMetadata tableMetadata) {
      this.root = new ObjectStorageRecordGroupConverter(schema, tableMetadata);
    }

    @Override
    public ObjectStorageRecord getCurrentRecord() {
      return root.buildRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }

  private static final int FIELD_CATEGORY_ID = 0;
  private static final int FIELD_CATEGORY_PARTITION_KEY = 1;
  private static final int FIELD_CATEGORY_CLUSTERING_KEY = 2;
  private static final int FIELD_CATEGORY_VALUE = 3;

  private static class ObjectStorageRecordGroupConverter extends GroupConverter {
    private final Converter[] converters;
    private final String[] fieldNames;
    private final Object[] fieldValues;
    private final int[] fieldCategories;
    private final int partitionKeyCount;
    private final int clusteringKeyCount;
    private final int valueCount;

    ObjectStorageRecordGroupConverter(MessageType schema, TableMetadata tableMetadata) {
      int fieldCount = schema.getFieldCount();
      this.converters = new Converter[fieldCount];
      this.fieldNames = new String[fieldCount];
      this.fieldValues = new Object[fieldCount];
      this.fieldCategories = new int[fieldCount];

      Set<String> partitionKeyNames = tableMetadata.getPartitionKeyNames();
      Set<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
      int pkCount = 0;
      int ckCount = 0;
      int valCount = 0;

      for (int i = 0; i < fieldCount; i++) {
        fieldNames[i] = schema.getFieldName(i);
        final int index = i;
        String columnName = fieldNames[i];

        if (columnName.equals("id")) {
          converters[i] = new StringConverter(index);
          fieldCategories[i] = FIELD_CATEGORY_ID;
        } else {
          DataType dataType = tableMetadata.getColumnDataType(columnName);
          converters[i] = createConverter(index, dataType);
          if (partitionKeyNames.contains(columnName)) {
            fieldCategories[i] = FIELD_CATEGORY_PARTITION_KEY;
            pkCount++;
          } else if (clusteringKeyNames.contains(columnName)) {
            fieldCategories[i] = FIELD_CATEGORY_CLUSTERING_KEY;
            ckCount++;
          } else {
            fieldCategories[i] = FIELD_CATEGORY_VALUE;
            valCount++;
          }
        }
      }
      this.partitionKeyCount = pkCount;
      this.clusteringKeyCount = ckCount;
      this.valueCount = valCount;
    }

    private Converter createConverter(int index, DataType dataType) {
      switch (dataType) {
        case BOOLEAN:
          return new BooleanFieldConverter(index);
        case INT:
        case DATE:
          return new IntFieldConverter(index);
        case BIGINT:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMPTZ:
          return new LongFieldConverter(index);
        case FLOAT:
          return new FloatFieldConverter(index);
        case DOUBLE:
          return new DoubleFieldConverter(index);
        case TEXT:
          return new StringConverter(index);
        case BLOB:
          return new BinaryConverter(index);
        default:
          throw new AssertionError("Unsupported data type: " + dataType);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      java.util.Arrays.fill(fieldValues, null);
    }

    @Override
    public void end() {}

    ObjectStorageRecord buildRecord() {
      String id = "";
      Map<String, Object> partitionKey = new LinkedHashMap<>(partitionKeyCount * 2);
      Map<String, Object> clusteringKey = new LinkedHashMap<>(clusteringKeyCount * 2);
      Map<String, Object> values = new HashMap<>(valueCount * 2);

      for (int i = 0; i < fieldNames.length; i++) {
        String name = fieldNames[i];
        Object value = fieldValues[i];

        switch (fieldCategories[i]) {
          case FIELD_CATEGORY_ID:
            id = value != null ? (String) value : "";
            break;
          case FIELD_CATEGORY_PARTITION_KEY:
            partitionKey.put(name, value);
            break;
          case FIELD_CATEGORY_CLUSTERING_KEY:
            clusteringKey.put(name, value);
            break;
          default:
            values.put(name, value);
            break;
        }
      }

      return ObjectStorageRecord.newBuilder()
          .id(id)
          .partitionKey(partitionKey)
          .clusteringKey(clusteringKey)
          .values(values)
          .build();
    }

    private class BooleanFieldConverter extends PrimitiveConverter {
      private final int index;

      BooleanFieldConverter(int index) {
        this.index = index;
      }

      @Override
      public void addBoolean(boolean value) {
        fieldValues[index] = value;
      }
    }

    private class IntFieldConverter extends PrimitiveConverter {
      private final int index;

      IntFieldConverter(int index) {
        this.index = index;
      }

      @Override
      public void addInt(int value) {
        fieldValues[index] = value;
      }
    }

    private class LongFieldConverter extends PrimitiveConverter {
      private final int index;

      LongFieldConverter(int index) {
        this.index = index;
      }

      @Override
      public void addLong(long value) {
        fieldValues[index] = value;
      }
    }

    private class FloatFieldConverter extends PrimitiveConverter {
      private final int index;

      FloatFieldConverter(int index) {
        this.index = index;
      }

      @Override
      public void addFloat(float value) {
        fieldValues[index] = value;
      }
    }

    private class DoubleFieldConverter extends PrimitiveConverter {
      private final int index;

      DoubleFieldConverter(int index) {
        this.index = index;
      }

      @Override
      public void addDouble(double value) {
        fieldValues[index] = value;
      }
    }

    private class StringConverter extends PrimitiveConverter {
      private final int index;

      StringConverter(int index) {
        this.index = index;
      }

      @Override
      public void addBinary(Binary value) {
        fieldValues[index] = value.toStringUsingUTF8();
      }
    }

    private class BinaryConverter extends PrimitiveConverter {
      private final int index;

      BinaryConverter(int index) {
        this.index = index;
      }

      @Override
      public void addBinary(Binary value) {
        fieldValues[index] = value.getBytes();
      }
    }
  }
}
