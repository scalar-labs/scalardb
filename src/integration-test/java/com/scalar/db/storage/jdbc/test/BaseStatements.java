package com.scalar.db.storage.jdbc.test;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.KeyType;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public interface BaseStatements {

  static String insertMetadataStatement(
      Optional<String> schemaPrefix,
      String fullTableName,
      String columnName,
      DataType dataType,
      @Nullable KeyType keyType,
      @Nullable Scan.Ordering.Order keyOrder,
      boolean indexed,
      int ordinalPosition) {
    return String.format(
        "INSERT INTO %s VALUES('%s','%s','%s',%s,%s,%s,%d)",
        TableMetadataManager.getFullTableName(schemaPrefix),
        fullTableName,
        columnName,
        dataType,
        keyType != null ? "'" + keyType + "'" : "NULL",
        keyOrder != null ? "'" + keyOrder + "'" : "NULL",
        indexed,
        ordinalPosition);
  }

  List<String> insertMetadataStatements(Optional<String> schemaPrefix);

  List<String> schemas(Optional<String> schemaPrefix);

  List<String> tables(Optional<String> schemaPrefix);

  List<String> createTableStatements(Optional<String> schemaPrefix);
}
