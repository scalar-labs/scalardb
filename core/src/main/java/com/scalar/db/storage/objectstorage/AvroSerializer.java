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

  public static byte[] serialize(
      ObjectStoragePartition partition, ObjectStorageTableMetadata metadata) {
    try {
      Schema recordSchema = schemaGenerator.getRecordSchema(metadata);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(recordSchema);
      try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter)) {
        fileWriter.setCodec(CodecFactory.snappyCodec());
        fileWriter.create(recordSchema, out);
        for (ObjectStorageRecord record : partition.getRecords().values()) {
          fileWriter.append(toAvroRecord(record, recordSchema, metadata));
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
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(recordSchema);
      try (DataFileReader<GenericRecord> fileReader =
          new DataFileReader<>(new SeekableByteArrayInput(data), datumReader)) {
        Map<String, ObjectStorageRecord> records = new HashMap<>();
        while (fileReader.hasNext()) {
          GenericRecord avroRecord = fileReader.next();
          ObjectStorageRecord record = fromAvroRecord(avroRecord, metadata);
          records.put(record.getId(), record);
        }
        return new ObjectStoragePartition(records);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize partition from Avro", e);
    }
  }

  private static GenericRecord toAvroRecord(
      ObjectStorageRecord record, Schema recordSchema, ObjectStorageTableMetadata metadata) {
    GenericRecord avroRecord = new GenericData.Record(recordSchema);
    avroRecord.put("id", record.getId());

    // Merge all column values (partitionKey + clusteringKey + values) and write as flat fields
    Map<String, Object> allValues = new HashMap<>();
    allValues.putAll(record.getPartitionKey());
    allValues.putAll(record.getClusteringKey());
    allValues.putAll(record.getValues());

    for (Map.Entry<String, String> columnEntry : metadata.getColumns().entrySet()) {
      String columnName = columnEntry.getKey();
      String columnType = columnEntry.getValue();
      String fieldName = sanitizeFieldName(columnName);
      Object value = allValues.get(columnName);

      if (value == null) {
        avroRecord.put(fieldName, null);
      } else if ("blob".equals(columnType)) {
        // Convert byte[] to ByteBuffer for Avro bytes type
        if (value instanceof byte[]) {
          avroRecord.put(fieldName, ByteBuffer.wrap((byte[]) value));
        } else if (value instanceof ByteBuffer) {
          avroRecord.put(fieldName, value);
        } else {
          avroRecord.put(fieldName, value);
        }
      } else {
        avroRecord.put(fieldName, value);
      }
    }

    return avroRecord;
  }

  private static ObjectStorageRecord fromAvroRecord(
      GenericRecord avroRecord, ObjectStorageTableMetadata metadata) {
    String id = avroRecord.get("id").toString();

    LinkedHashSet<String> partitionKeyNames = metadata.getPartitionKeyNames();
    LinkedHashSet<String> clusteringKeyNames = metadata.getClusteringKeyNames();

    Map<String, Object> partitionKey = new HashMap<>();
    Map<String, Object> clusteringKey = new HashMap<>();
    Map<String, Object> values = new HashMap<>();

    for (Map.Entry<String, String> columnEntry : metadata.getColumns().entrySet()) {
      String columnName = columnEntry.getKey();
      String fieldName = sanitizeFieldName(columnName);
      Object value = avroRecord.get(fieldName);

      // Convert Avro types back to Java types
      value = convertAvroValue(value);

      if (partitionKeyNames.contains(columnName)) {
        partitionKey.put(columnName, value);
      } else if (clusteringKeyNames.contains(columnName)) {
        clusteringKey.put(columnName, value);
      } else {
        values.put(columnName, value);
      }
    }

    return ObjectStorageRecord.newBuilder()
        .id(id)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .values(values)
        .build();
  }

  private static Object convertAvroValue(Object value) {
    if (value == null) {
      return null;
    }

    // Avro returns Utf8 for strings; convert to String
    if (value instanceof Utf8) {
      return value.toString();
    }

    // Avro returns ByteBuffer for bytes; convert to byte[]
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
