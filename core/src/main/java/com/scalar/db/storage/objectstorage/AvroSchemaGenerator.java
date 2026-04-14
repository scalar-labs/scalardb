package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

@ThreadSafe
class AvroSchemaGenerator {

  private final ConcurrentHashMap<String, Schema> schemaCache = new ConcurrentHashMap<>();

  Schema getRecordSchema(TableMetadata metadata) {
    String cacheKey = buildCacheKey(metadata);
    return schemaCache.computeIfAbsent(cacheKey, k -> buildRecordSchema(metadata));
  }

  private Schema buildRecordSchema(TableMetadata metadata) {
    SchemaBuilder.FieldAssembler<Schema> fields =
        SchemaBuilder.record("ObjectStorageRecord")
            .namespace("com.scalar.db.objectstorage")
            .fields();

    // id field (non-null)
    fields = fields.requiredString("id");

    // Partition key columns
    for (String columnName : metadata.getPartitionKeyNames()) {
      DataType dataType = metadata.getColumnDataType(columnName);
      Schema valueSchema = toAvroSchema(dataType);
      Schema nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema);
      fields = fields.name(sanitizeFieldName(columnName)).type(nullableSchema).withDefault(null);
    }

    // Clustering key columns
    for (String columnName : metadata.getClusteringKeyNames()) {
      DataType dataType = metadata.getColumnDataType(columnName);
      Schema valueSchema = toAvroSchema(dataType);
      Schema nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema);
      fields = fields.name(sanitizeFieldName(columnName)).type(nullableSchema).withDefault(null);
    }

    // Value columns (non-key)
    Set<String> partitionKeyNames = metadata.getPartitionKeyNames();
    Set<String> clusteringKeyNames = metadata.getClusteringKeyNames();
    for (String columnName : metadata.getColumnNames()) {
      if (!partitionKeyNames.contains(columnName) && !clusteringKeyNames.contains(columnName)) {
        DataType dataType = metadata.getColumnDataType(columnName);
        Schema valueSchema = toAvroSchema(dataType);
        Schema nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema);
        fields = fields.name(sanitizeFieldName(columnName)).type(nullableSchema).withDefault(null);
      }
    }

    return fields.endRecord();
  }

  private static Schema toAvroSchema(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case INT:
      case DATE:
        return Schema.create(Schema.Type.INT);
      case BIGINT:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case TEXT:
        return Schema.create(Schema.Type.STRING);
      case BLOB:
        return Schema.create(Schema.Type.BYTES);
      default:
        throw new AssertionError("Unsupported data type: " + dataType);
    }
  }

  private static String buildCacheKey(TableMetadata metadata) {
    StringBuilder sb = new StringBuilder();
    for (String columnName : metadata.getPartitionKeyNames()) {
      sb.append("pk:")
          .append(columnName)
          .append(':')
          .append(metadata.getColumnDataType(columnName))
          .append(';');
    }
    for (String columnName : metadata.getClusteringKeyNames()) {
      sb.append("ck:")
          .append(columnName)
          .append(':')
          .append(metadata.getColumnDataType(columnName))
          .append(';');
    }
    for (String columnName : metadata.getColumnNames()) {
      if (!metadata.getPartitionKeyNames().contains(columnName)
          && !metadata.getClusteringKeyNames().contains(columnName)) {
        sb.append("v:")
            .append(columnName)
            .append(':')
            .append(metadata.getColumnDataType(columnName))
            .append(';');
      }
    }
    return sb.toString();
  }

  static String sanitizeFieldName(String name) {
    String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "_" + sanitized;
    }
    return sanitized;
  }
}
