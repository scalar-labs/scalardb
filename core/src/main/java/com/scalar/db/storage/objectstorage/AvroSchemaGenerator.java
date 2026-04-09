package com.scalar.db.storage.objectstorage;

import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

@ThreadSafe
class AvroSchemaGenerator {

  Schema getRecordSchema(ObjectStorageTableMetadata metadata) {
    SchemaBuilder.FieldAssembler<Schema> fields =
        SchemaBuilder.record("ObjectStorageRecord")
            .namespace("com.scalar.db.objectstorage")
            .fields();

    // id field (non-null)
    fields = fields.requiredString("id");

    // Add all columns as nullable fields
    for (Map.Entry<String, String> entry : metadata.getColumns().entrySet()) {
      String columnName = entry.getKey();
      String columnType = entry.getValue();
      Schema valueSchema = toAvroSchema(columnType);
      Schema nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema);
      fields = fields.name(sanitizeFieldName(columnName)).type(nullableSchema).withDefault(null);
    }

    return fields.endRecord();
  }

  private static Schema toAvroSchema(String scalarDbType) {
    switch (scalarDbType) {
      case "boolean":
        return Schema.create(Schema.Type.BOOLEAN);
      case "int":
      case "date":
        return Schema.create(Schema.Type.INT);
      case "bigint":
      case "time":
      case "timestamp":
      case "timestamptz":
        return Schema.create(Schema.Type.LONG);
      case "float":
        return Schema.create(Schema.Type.FLOAT);
      case "double":
        return Schema.create(Schema.Type.DOUBLE);
      case "text":
        return Schema.create(Schema.Type.STRING);
      case "blob":
        return Schema.create(Schema.Type.BYTES);
      default:
        throw new AssertionError("Unknown ScalarDB data type: " + scalarDbType);
    }
  }

  private static String sanitizeFieldName(String name) {
    String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "_" + sanitized;
    }
    return sanitized;
  }
}
