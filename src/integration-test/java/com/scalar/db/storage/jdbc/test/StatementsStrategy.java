package com.scalar.db.storage.jdbc.test;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.KeyType;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import javax.annotation.Nullable;
import java.util.List;

public interface StatementsStrategy {
  List<String> insertMetadataStatements(String schemaPrefix);

  static String insertMetaColumnsTableStatement(
      String schemaPrefix,
      String schema,
      String table,
      String columnName,
      DataType dataType,
      @Nullable KeyType keyType,
      @Nullable Scan.Ordering.Order keyOrder,
      boolean indexed,
      int ordinalPosition) {
    return String.format(
        "INSERT INTO "
            + TableMetadataManager.getTable(schemaPrefix)
            + " VALUES('%s','%s','%s','%s',%s,%s,%s,%d)",
        schema,
        table,
        columnName,
        dataType,
        keyType != null ? "'" + keyType + "'" : "NULL",
        keyOrder != null ? "'" + keyOrder + "'" : "NULL",
        indexed,
        ordinalPosition);
  }

  List<String> dataSchemas(String schemaPrefix);

  List<String> dataTables(String schemaPrefix);

  List<String> createDataTableStatements(String schemaPrefix);
}
