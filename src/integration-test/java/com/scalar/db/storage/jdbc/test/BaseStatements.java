package com.scalar.db.storage.jdbc.test;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.KeyType;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public interface BaseStatements {

  static String insertMetadataStatement(
      Optional<String> namespacePrefix,
      String fullTableName,
      String columnName,
      DataType dataType,
      @Nullable KeyType keyType,
      @Nullable Scan.Ordering.Order keyOrder,
      boolean indexed,
      @Nullable Scan.Ordering.Order indexOrder,
      int ordinalPosition) {
    return String.format(
        "INSERT INTO %s VALUES('%s','%s','%s',%s,%s,%s,%s,%d)",
        TableMetadataManager.getFullTableName(namespacePrefix),
        fullTableName,
        columnName,
        dataType,
        keyType != null ? "'" + keyType + "'" : "NULL",
        keyOrder != null ? "'" + keyOrder + "'" : "NULL",
        indexed,
        indexOrder != null ? "'" + indexOrder + "'" : "NULL",
        ordinalPosition);
  }

  List<String> insertMetadataStatements(Optional<String> namespacePrefix);

  List<String> schemas(Optional<String> namespacePrefix, RdbEngine rdbEngine);

  List<String> tables(Optional<String> namespacePrefix, RdbEngine rdbEngine);

  List<String> createTableStatements(Optional<String> namespacePrefix, RdbEngine rdbEngine);
}
