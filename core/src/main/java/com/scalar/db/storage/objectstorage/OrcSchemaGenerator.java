package com.scalar.db.storage.objectstorage;

import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.orc.TypeDescription;

@ThreadSafe
class OrcSchemaGenerator {

  TypeDescription getRecordSchema(ObjectStorageTableMetadata metadata) {
    TypeDescription schema = TypeDescription.createStruct();

    // id field (non-null string)
    schema.addField("id", TypeDescription.createString());

    // Add all columns as fields
    for (Map.Entry<String, String> entry : metadata.getColumns().entrySet()) {
      String columnName = entry.getKey();
      String columnType = entry.getValue();
      schema.addField(sanitizeFieldName(columnName), toOrcType(columnType));
    }

    return schema;
  }

  private static TypeDescription toOrcType(String scalarDbType) {
    switch (scalarDbType) {
      case "boolean":
        return TypeDescription.createBoolean();
      case "int":
      case "date":
        return TypeDescription.createInt();
      case "bigint":
      case "time":
      case "timestamp":
      case "timestamptz":
        return TypeDescription.createLong();
      case "float":
        return TypeDescription.createFloat();
      case "double":
        return TypeDescription.createDouble();
      case "text":
        return TypeDescription.createString();
      case "blob":
        return TypeDescription.createBinary();
      default:
        throw new AssertionError("Unknown ScalarDB data type: " + scalarDbType);
    }
  }

  static String sanitizeFieldName(String name) {
    String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "_" + sanitized;
    }
    return sanitized;
  }
}
