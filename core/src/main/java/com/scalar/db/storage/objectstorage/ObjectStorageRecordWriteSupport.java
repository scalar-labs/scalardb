package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ObjectStorageRecordWriteSupport extends WriteSupport<ObjectStorageRecord> {
  private static final int SOURCE_PARTITION_KEY = 0;
  private static final int SOURCE_CLUSTERING_KEY = 1;
  private static final int SOURCE_VALUES = 2;

  private final MessageType schema;
  // Pre-computed field descriptors (excluding the id field at index 0)
  private final String[] fieldNames;
  private final DataType[] fieldDataTypes;
  private final int[] fieldSources; // SOURCE_PARTITION_KEY, SOURCE_CLUSTERING_KEY, or SOURCE_VALUES
  private final int fieldCount;
  private RecordConsumer recordConsumer;

  public ObjectStorageRecordWriteSupport(MessageType schema, TableMetadata tableMetadata) {
    this.schema = schema;

    List<String> names = new ArrayList<>();
    List<DataType> types = new ArrayList<>();
    List<Integer> sources = new ArrayList<>();

    for (String columnName : tableMetadata.getPartitionKeyNames()) {
      names.add(columnName);
      types.add(tableMetadata.getColumnDataType(columnName));
      sources.add(SOURCE_PARTITION_KEY);
    }
    for (String columnName : tableMetadata.getClusteringKeyNames()) {
      names.add(columnName);
      types.add(tableMetadata.getColumnDataType(columnName));
      sources.add(SOURCE_CLUSTERING_KEY);
    }
    for (Map.Entry<String, DataType> entry : getValueColumns(tableMetadata).entrySet()) {
      names.add(entry.getKey());
      types.add(entry.getValue());
      sources.add(SOURCE_VALUES);
    }

    this.fieldCount = names.size();
    this.fieldNames = names.toArray(new String[0]);
    this.fieldDataTypes = types.toArray(new DataType[0]);
    this.fieldSources = new int[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      this.fieldSources[i] = sources.get(i);
    }
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext(schema, new HashMap<>());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(ObjectStorageRecord record) {
    recordConsumer.startMessage();

    // Write id field (required binary id (UTF8)) at index 0
    recordConsumer.startField("id", 0);
    recordConsumer.addBinary(Binary.fromString(record.getId()));
    recordConsumer.endField("id", 0);

    Map<String, Object> partitionKey = record.getPartitionKey();
    Map<String, Object> clusteringKey = record.getClusteringKey();
    Map<String, Object> values = record.getValues();

    for (int i = 0; i < fieldCount; i++) {
      int fieldIndex = i + 1; // offset by 1 for the id field
      Object value;
      switch (fieldSources[i]) {
        case SOURCE_PARTITION_KEY:
          value = partitionKey.get(fieldNames[i]);
          break;
        case SOURCE_CLUSTERING_KEY:
          value = clusteringKey.get(fieldNames[i]);
          break;
        default:
          value = values.get(fieldNames[i]);
          break;
      }
      if (value == null) {
        continue;
      }
      writeField(fieldNames[i], fieldIndex, fieldDataTypes[i], value);
    }

    recordConsumer.endMessage();
  }

  private void writeField(String name, int fieldIndex, DataType dataType, Object value) {
    recordConsumer.startField(name, fieldIndex);
    switch (dataType) {
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) value);
        break;
      case INT:
      case DATE:
        recordConsumer.addInteger(((Number) value).intValue());
        break;
      case BIGINT:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        recordConsumer.addLong(((Number) value).longValue());
        break;
      case FLOAT:
        recordConsumer.addFloat(((Number) value).floatValue());
        break;
      case DOUBLE:
        recordConsumer.addDouble(((Number) value).doubleValue());
        break;
      case TEXT:
        recordConsumer.addBinary(Binary.fromString((String) value));
        break;
      case BLOB:
        if (value instanceof byte[]) {
          recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) value));
        } else if (value instanceof ByteBuffer) {
          ByteBuffer buffer = ((ByteBuffer) value).duplicate();
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          recordConsumer.addBinary(Binary.fromConstantByteArray(bytes));
        } else {
          throw new AssertionError("Unsupported BLOB value type: " + value.getClass());
        }
        break;
      default:
        throw new AssertionError("Unsupported data type: " + dataType);
    }
    recordConsumer.endField(name, fieldIndex);
  }

  static Map<String, DataType> getValueColumns(TableMetadata tableMetadata) {
    Map<String, DataType> valueColumns = new LinkedHashMap<>();
    for (String columnName : tableMetadata.getColumnNames()) {
      if (!tableMetadata.getPartitionKeyNames().contains(columnName)
          && !tableMetadata.getClusteringKeyNames().contains(columnName)) {
        valueColumns.put(columnName, tableMetadata.getColumnDataType(columnName));
      }
    }
    return valueColumns;
  }
}
