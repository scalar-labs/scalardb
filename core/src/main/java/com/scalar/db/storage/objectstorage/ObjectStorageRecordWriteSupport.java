package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ObjectStorageRecordWriteSupport extends WriteSupport<ObjectStorageRecord> {
  private final MessageType schema;
  private final TableMetadata tableMetadata;
  private final Map<String, DataType> valueColumns;
  private RecordConsumer recordConsumer;

  public ObjectStorageRecordWriteSupport(MessageType schema, TableMetadata tableMetadata) {
    this.schema = schema;
    this.tableMetadata = tableMetadata;
    this.valueColumns = getValueColumns(tableMetadata);
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

    int fieldIndex = 0;

    // Write id field (required binary id (UTF8))
    recordConsumer.startField("id", fieldIndex);
    recordConsumer.addBinary(Binary.fromString(record.getId()));
    recordConsumer.endField("id", fieldIndex);
    fieldIndex++;

    // Write partition key columns
    for (String columnName : tableMetadata.getPartitionKeyNames()) {
      DataType dataType = tableMetadata.getColumnDataType(columnName);
      Object value = record.getPartitionKey().get(columnName);
      writeField(columnName, fieldIndex, dataType, value);
      fieldIndex++;
    }

    // Write clustering key columns
    for (String columnName : tableMetadata.getClusteringKeyNames()) {
      DataType dataType = tableMetadata.getColumnDataType(columnName);
      Object value = record.getClusteringKey().get(columnName);
      writeField(columnName, fieldIndex, dataType, value);
      fieldIndex++;
    }

    // Write value columns (non-key columns)
    Map<String, Object> values = record.getValues();
    for (Map.Entry<String, DataType> entry : valueColumns.entrySet()) {
      String columnName = entry.getKey();
      DataType dataType = entry.getValue();
      Object value = values.get(columnName);
      writeField(columnName, fieldIndex, dataType, value);
      fieldIndex++;
    }

    recordConsumer.endMessage();
  }

  private void writeField(String name, int fieldIndex, DataType dataType, Object value) {
    if (value == null) {
      return; // optional field, skip null
    }
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
    Map<String, DataType> valueColumns = new java.util.LinkedHashMap<>();
    for (String columnName : tableMetadata.getColumnNames()) {
      if (!tableMetadata.getPartitionKeyNames().contains(columnName)
          && !tableMetadata.getClusteringKeyNames().contains(columnName)) {
        valueColumns.put(columnName, tableMetadata.getColumnDataType(columnName));
      }
    }
    return valueColumns;
  }
}
