package com.scalar.db.storage.objectstorage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

@ThreadSafe
public class AvroSerializer {
  private static final AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();

  private static final int CATEGORY_PARTITION_KEY = 0;
  private static final int CATEGORY_CLUSTERING_KEY = 1;
  private static final int CATEGORY_VALUE = 2;

  public static byte[] serialize(
      ObjectStoragePartition partition, ObjectStorageTableMetadata metadata) {
    return serialize(
        partition, metadata, CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL));
  }

  static byte[] serialize(
      ObjectStoragePartition partition,
      ObjectStorageTableMetadata metadata,
      CodecFactory codecFactory) {
    try {
      Schema recordSchema = schemaGenerator.getRecordSchema(metadata);

      // Precompute column info
      Map<String, String> columns = metadata.getColumns();
      int columnCount = columns.size();
      String[] columnNames = new String[columnCount];
      String[] columnTypes = new String[columnCount];
      int[] fieldIndices = new int[columnCount];
      int[] categories = new int[columnCount];

      LinkedHashSet<String> pkNames = metadata.getPartitionKeyNames();
      LinkedHashSet<String> ckNames = metadata.getClusteringKeyNames();
      int idx = 0;
      for (Map.Entry<String, String> entry : columns.entrySet()) {
        String name = entry.getKey();
        columnNames[idx] = name;
        columnTypes[idx] = entry.getValue();
        fieldIndices[idx] = recordSchema.getField(sanitizeFieldName(name)).pos();
        if (pkNames.contains(name)) {
          categories[idx] = CATEGORY_PARTITION_KEY;
        } else if (ckNames.contains(name)) {
          categories[idx] = CATEGORY_CLUSTERING_KEY;
        } else {
          categories[idx] = CATEGORY_VALUE;
        }
        idx++;
      }
      int idFieldIndex = recordSchema.getField("id").pos();

      int estimatedSize = estimateOutputSize(partition, metadata);
      ByteArrayOutputStream out = new ByteArrayOutputStream(estimatedSize);
      GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(recordSchema);
      try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter)) {
        fileWriter.setCodec(codecFactory);
        fileWriter.create(recordSchema, out);

        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        for (ObjectStorageRecord record : partition.getRecords().values()) {
          avroRecord.put(idFieldIndex, record.getId());

          Map<String, Object> pkMap = record.getPartitionKey();
          Map<String, Object> ckMap = record.getClusteringKey();
          Map<String, Object> valMap = record.getValues();

          for (int i = 0; i < columnCount; i++) {
            Object value;
            switch (categories[i]) {
              case CATEGORY_PARTITION_KEY:
                value = pkMap.get(columnNames[i]);
                break;
              case CATEGORY_CLUSTERING_KEY:
                value = ckMap.get(columnNames[i]);
                break;
              default:
                value = valMap.get(columnNames[i]);
                break;
            }

            if (value != null && "blob".equals(columnTypes[i]) && value instanceof byte[]) {
              value = ByteBuffer.wrap((byte[]) value);
            }
            avroRecord.put(fieldIndices[i], value);
          }

          fileWriter.append(avroRecord);
        }
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize partition to Avro", e);
    }
  }

  public static ObjectStoragePartition deserialize(
      byte[] data, ObjectStorageTableMetadata metadata) {
    try {
      Schema recordSchema = schemaGenerator.getRecordSchema(metadata);

      // Precompute column info
      Map<String, String> columns = metadata.getColumns();
      int columnCount = columns.size();
      String[] columnNames = new String[columnCount];
      int[] fieldIndices = new int[columnCount];
      int[] categories = new int[columnCount];

      LinkedHashSet<String> pkNames = metadata.getPartitionKeyNames();
      LinkedHashSet<String> ckNames = metadata.getClusteringKeyNames();
      int pkCount = pkNames.size();
      int ckCount = ckNames.size();
      int valueCount = columnCount - pkCount - ckCount;

      int idx = 0;
      for (Map.Entry<String, String> entry : columns.entrySet()) {
        String name = entry.getKey();
        columnNames[idx] = name;
        fieldIndices[idx] = recordSchema.getField(sanitizeFieldName(name)).pos();
        if (pkNames.contains(name)) {
          categories[idx] = CATEGORY_PARTITION_KEY;
        } else if (ckNames.contains(name)) {
          categories[idx] = CATEGORY_CLUSTERING_KEY;
        } else {
          categories[idx] = CATEGORY_VALUE;
        }
        idx++;
      }
      int idFieldIndex = recordSchema.getField("id").pos();

      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(recordSchema);
      try (DataFileReader<GenericRecord> fileReader =
          new DataFileReader<>(new SeekableByteArrayInput(data), datumReader)) {
        Map<String, ObjectStorageRecord> records = new HashMap<>();
        GenericRecord reuse = null;
        while (fileReader.hasNext()) {
          GenericRecord avroRecord = fileReader.next(reuse);
          reuse = avroRecord;

          String id = avroRecord.get(idFieldIndex).toString();

          Map<String, Object> partitionKey = new HashMap<>(pkCount);
          Map<String, Object> clusteringKey = new HashMap<>(ckCount);
          Map<String, Object> values = new HashMap<>(valueCount);

          for (int i = 0; i < columnCount; i++) {
            Object value = convertAvroValue(avroRecord.get(fieldIndices[i]));
            switch (categories[i]) {
              case CATEGORY_PARTITION_KEY:
                partitionKey.put(columnNames[i], value);
                break;
              case CATEGORY_CLUSTERING_KEY:
                clusteringKey.put(columnNames[i], value);
                break;
              default:
                values.put(columnNames[i], value);
                break;
            }
          }

          records.put(
              id,
              ObjectStorageRecord.newBuilder()
                  .id(id)
                  .partitionKey(partitionKey)
                  .clusteringKey(clusteringKey)
                  .values(values)
                  .build());
        }
        return new ObjectStoragePartition(records);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize partition from Avro", e);
    }
  }

  private static int estimateOutputSize(
      ObjectStoragePartition partition, ObjectStorageTableMetadata metadata) {
    int recordCount = partition.getRecords().size();
    if (recordCount == 0) {
      return 1024;
    }

    // Estimate bytes per record based on column data types
    long bytesPerRecord = 0;
    for (String columnType : metadata.getColumns().values()) {
      switch (columnType) {
        case "boolean":
          bytesPerRecord += 1;
          break;
        case "int":
        case "float":
        case "date":
          bytesPerRecord += 4;
          break;
        case "bigint":
        case "double":
        case "time":
        case "timestamp":
        case "timestamptz":
          bytesPerRecord += 8;
          break;
        case "text":
          bytesPerRecord += 64;
          break;
        case "blob":
          bytesPerRecord += 1024;
          break;
        default:
          bytesPerRecord += 32;
          break;
      }
    }

    long estimated = (long) recordCount * bytesPerRecord;
    // Clamp to int range (ByteArrayOutputStream uses int)
    return (int) Math.min(estimated, Integer.MAX_VALUE - 8);
  }

  private static Object convertAvroValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Utf8) {
      return value.toString();
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buffer = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    }
    return value;
  }

  private static String sanitizeFieldName(String name) {
    String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "_" + sanitized;
    }
    return sanitized;
  }
}
