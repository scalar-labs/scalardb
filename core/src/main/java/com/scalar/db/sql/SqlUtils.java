package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.scalar.db.sql.Predicate.Operator;
import com.scalar.db.sql.exception.TableNotFoundException;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.metadata.TableMetadata;

public final class SqlUtils {

  private SqlUtils() {}

  public static boolean isIndexScan(
      ImmutableListMultimap<String, Predicate> predicatesMap, TableMetadata tableMetadata) {
    if (predicatesMap.size() != 1) {
      return false;
    }
    String columnName = predicatesMap.keySet().iterator().next();
    if (!tableMetadata.getIndex(columnName).isPresent()) {
      return false;
    }
    ImmutableList<Predicate> predicates = predicatesMap.get(columnName);
    if (predicates.size() != 1) {
      return false;
    }
    return predicates.get(0).operator == Operator.EQUAL_TO;
  }

  public static TableMetadata getTableMetadata(
      Metadata metadata, String namespaceName, String tableName) {
    return metadata
        .getNamespace(namespaceName)
        .orElseThrow(() -> new TableNotFoundException(namespaceName, tableName))
        .getTable(tableName)
        .orElseThrow(() -> new TableNotFoundException(namespaceName, tableName));
  }
}
