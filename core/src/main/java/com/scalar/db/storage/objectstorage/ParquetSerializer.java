package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetSerializer {

  private static final AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();

  private static final int CATEGORY_PARTITION_KEY = 0;
  private static final int CATEGORY_CLUSTERING_KEY = 1;
  private static final int CATEGORY_VALUE = 2;

  private ParquetSerializer() {}

  private static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.ZSTD;

  public static byte[] serialize(ObjectStoragePartition partition, TableMetadata metadata) {
    return serialize(partition, metadata, DEFAULT_COMPRESSION);
  }

  static byte[] serialize(
      ObjectStoragePartition partition,
      TableMetadata metadata,
      CompressionCodecName compressionCodec) {
    Schema recordSchema = schemaGenerator.getRecordSchema(metadata);
    int estimatedSize = estimateOutputSize(partition, metadata);
    InMemoryOutputFile outputFile = new InMemoryOutputFile(estimatedSize);

    // Precompute column info
    Set<String> partitionKeyNames = metadata.getPartitionKeyNames();
    Set<String> clusteringKeyNames = metadata.getClusteringKeyNames();

    // Build ordered column arrays: PK → CK → values (matching schema field order)
    int totalColumns = 0;
    for (String ignored : metadata.getColumnNames()) {
      totalColumns++;
    }
    String[] columnNames = new String[totalColumns];
    DataType[] columnTypes = new DataType[totalColumns];
    int[] fieldIndices = new int[totalColumns];
    int[] categories = new int[totalColumns];

    int idx = 0;
    for (String name : metadata.getPartitionKeyNames()) {
      columnNames[idx] = name;
      columnTypes[idx] = metadata.getColumnDataType(name);
      fieldIndices[idx] = recordSchema.getField(AvroSchemaGenerator.sanitizeFieldName(name)).pos();
      categories[idx] = CATEGORY_PARTITION_KEY;
      idx++;
    }
    for (String name : metadata.getClusteringKeyNames()) {
      columnNames[idx] = name;
      columnTypes[idx] = metadata.getColumnDataType(name);
      fieldIndices[idx] = recordSchema.getField(AvroSchemaGenerator.sanitizeFieldName(name)).pos();
      categories[idx] = CATEGORY_CLUSTERING_KEY;
      idx++;
    }
    for (String name : metadata.getColumnNames()) {
      if (!partitionKeyNames.contains(name) && !clusteringKeyNames.contains(name)) {
        columnNames[idx] = name;
        columnTypes[idx] = metadata.getColumnDataType(name);
        fieldIndices[idx] =
            recordSchema.getField(AvroSchemaGenerator.sanitizeFieldName(name)).pos();
        categories[idx] = CATEGORY_VALUE;
        idx++;
      }
    }
    int columnCount = idx;
    int idFieldIndex = recordSchema.getField("id").pos();

    AvroParquetWriter.Builder<GenericRecord> builder =
        AvroParquetWriter.<GenericRecord>builder(outputFile)
            .withSchema(recordSchema)
            .withCompressionCodec(compressionCodec)
            .withDictionaryEncoding(true)
            .withRowGroupSize(128 * 1024 * 1024L)
            .withPageSize(256 * 1024)
            .withValidation(false)
            .withConf(new Configuration());

    // Disable dictionary encoding for BLOB columns
    for (String columnName : metadata.getColumnNames()) {
      if (metadata.getColumnDataType(columnName) == DataType.BLOB) {
        builder.withDictionaryEncoding(AvroSchemaGenerator.sanitizeFieldName(columnName), false);
      }
    }

    try (ParquetWriter<GenericRecord> writer = builder.build()) {
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

          if (value != null && columnTypes[i] == DataType.BLOB && value instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) value);
          }
          avroRecord.put(fieldIndices[i], value);
        }

        writer.write(avroRecord);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize partition to Parquet", e);
    }

    return outputFile.toByteArray();
  }

  public static ObjectStoragePartition deserialize(byte[] payload, TableMetadata metadata) {
    InMemoryInputFile inputFile = new InMemoryInputFile(payload);
    Schema recordSchema = schemaGenerator.getRecordSchema(metadata);

    // Precompute column info
    Set<String> partitionKeyNames = metadata.getPartitionKeyNames();
    Set<String> clusteringKeyNames = metadata.getClusteringKeyNames();
    int pkCount = partitionKeyNames.size();
    int ckCount = clusteringKeyNames.size();

    int totalColumns = 0;
    for (String ignored : metadata.getColumnNames()) {
      totalColumns++;
    }
    String[] columnNames = new String[totalColumns];
    int[] fieldIndices = new int[totalColumns];
    int[] categories = new int[totalColumns];

    int idx = 0;
    for (String name : metadata.getPartitionKeyNames()) {
      columnNames[idx] = name;
      fieldIndices[idx] = recordSchema.getField(AvroSchemaGenerator.sanitizeFieldName(name)).pos();
      categories[idx] = CATEGORY_PARTITION_KEY;
      idx++;
    }
    for (String name : metadata.getClusteringKeyNames()) {
      columnNames[idx] = name;
      fieldIndices[idx] = recordSchema.getField(AvroSchemaGenerator.sanitizeFieldName(name)).pos();
      categories[idx] = CATEGORY_CLUSTERING_KEY;
      idx++;
    }
    for (String name : metadata.getColumnNames()) {
      if (!partitionKeyNames.contains(name) && !clusteringKeyNames.contains(name)) {
        columnNames[idx] = name;
        fieldIndices[idx] =
            recordSchema.getField(AvroSchemaGenerator.sanitizeFieldName(name)).pos();
        categories[idx] = CATEGORY_VALUE;
        idx++;
      }
    }
    int columnCount = idx;
    int valueCount = columnCount - pkCount - ckCount;
    int idFieldIndex = recordSchema.getField("id").pos();

    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputFile).withConf(new Configuration()).build()) {
      Map<String, ObjectStorageRecord> records = new HashMap<>();
      GenericRecord avroRecord;
      while ((avroRecord = reader.read()) != null) {
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
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize partition from Parquet", e);
    }
  }

  private static int estimateOutputSize(ObjectStoragePartition partition, TableMetadata metadata) {
    int recordCount = partition.getRecords().size();
    if (recordCount == 0) {
      return 1024;
    }

    // Estimate bytes per record based on column data types
    long bytesPerRecord = 0;
    for (String columnName : metadata.getColumnNames()) {
      DataType dataType = metadata.getColumnDataType(columnName);
      switch (dataType) {
        case BOOLEAN:
          bytesPerRecord += 1;
          break;
        case INT:
        case FLOAT:
        case DATE:
          bytesPerRecord += 4;
          break;
        case BIGINT:
        case DOUBLE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMPTZ:
          bytesPerRecord += 8;
          break;
        case TEXT:
          bytesPerRecord += 64; // conservative estimate for text columns
          break;
        case BLOB:
          bytesPerRecord += 1024; // conservative estimate for blob columns
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
}
