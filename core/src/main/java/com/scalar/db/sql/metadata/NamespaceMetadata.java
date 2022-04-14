package com.scalar.db.sql.metadata;

import java.util.Map;
import java.util.Optional;

public interface NamespaceMetadata {

  String getName();

  Map<String, TableMetadata> getTables();

  Optional<TableMetadata> getTable(String tableName);
}
