package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class ParquetSerializer {

  private ParquetSerializer() {}

  private static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.ZSTD;

  public static byte[] serialize(ObjectStoragePartition partition, TableMetadata metadata) {
    return serialize(partition, metadata, DEFAULT_COMPRESSION);
  }

  static byte[] serialize(
      ObjectStoragePartition partition,
      TableMetadata metadata,
      CompressionCodecName compressionCodec) {
    MessageType schema = buildSchema(metadata);
    InMemoryOutputFile outputFile = new InMemoryOutputFile();

    try (ParquetWriter<ObjectStorageRecord> writer =
        new ObjectStorageParquetWriterBuilder(
                outputFile, new ObjectStorageRecordWriteSupport(schema, metadata))
            .withCompressionCodec(compressionCodec)
            .withDictionaryEncoding(false)
            .withRowGroupSize(4 * 1024 * 1024L) // 4MB
            .withPageSize(256 * 1024) // 256KB
            .withValidation(false)
            .build()) {
      for (ObjectStorageRecord record : partition.getRecords().values()) {
        writer.write(record);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize partition to Parquet", e);
    }

    return outputFile.toByteArray();
  }

  public static ObjectStoragePartition deserialize(byte[] payload, TableMetadata metadata) {
    InMemoryInputFile inputFile = new InMemoryInputFile(payload);
    Map<String, ObjectStorageRecord> records = new HashMap<>();

    try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
      MessageType schema = fileReader.getFileMetaData().getSchema();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      PageReadStore pages;
      while ((pages = fileReader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        RecordReader<ObjectStorageRecord> recordReader =
            columnIO.getRecordReader(
                pages, ObjectStorageRecordReadSupport.createMaterializer(schema, metadata));
        for (long i = 0; i < rows; i++) {
          ObjectStorageRecord record = recordReader.read();
          records.put(record.getId(), record);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize partition from Parquet", e);
    }

    return new ObjectStoragePartition(records);
  }

  static MessageType buildSchema(TableMetadata metadata) {
    List<Type> fields = new ArrayList<>();

    // id field: required binary id (UTF8)
    fields.add(
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("id"));

    // Partition key columns
    for (String columnName : metadata.getPartitionKeyNames()) {
      fields.add(buildFieldType(columnName, metadata.getColumnDataType(columnName)));
    }

    // Clustering key columns
    for (String columnName : metadata.getClusteringKeyNames()) {
      fields.add(buildFieldType(columnName, metadata.getColumnDataType(columnName)));
    }

    // Value columns (non-key)
    for (Map.Entry<String, DataType> entry :
        ObjectStorageRecordWriteSupport.getValueColumns(metadata).entrySet()) {
      fields.add(buildFieldType(entry.getKey(), entry.getValue()));
    }

    return new MessageType("ObjectStorageRecord", fields);
  }

  private static Type buildFieldType(String name, DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
      case INT:
      case DATE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
      case BIGINT:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
      case FLOAT:
        return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
      case DOUBLE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
      case TEXT:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(name);
      case BLOB:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
      default:
        throw new AssertionError("Unsupported data type: " + dataType);
    }
  }

  private static class ObjectStorageParquetWriterBuilder
      extends ParquetWriter.Builder<ObjectStorageRecord, ObjectStorageParquetWriterBuilder> {

    private final WriteSupport<ObjectStorageRecord> writeSupport;

    ObjectStorageParquetWriterBuilder(
        OutputFile file, WriteSupport<ObjectStorageRecord> writeSupport) {
      super(file);
      this.writeSupport = writeSupport;
    }

    @Override
    protected ObjectStorageParquetWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<ObjectStorageRecord> getWriteSupport(Configuration conf) {
      return writeSupport;
    }
  }
}
