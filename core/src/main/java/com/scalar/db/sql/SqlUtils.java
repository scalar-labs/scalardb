package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.Predicate.Operator;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TableNotFoundException;

public final class SqlUtils {

  private SqlUtils() {}

  public static boolean isIndexScan(
      ImmutableListMultimap<String, Predicate> predicatesMap,
      com.scalar.db.api.TableMetadata metadata) {
    if (predicatesMap.size() != 1) {
      return false;
    }
    String columnName = predicatesMap.keySet().iterator().next();
    if (!metadata.getSecondaryIndexNames().contains(columnName)) {
      return false;
    }
    ImmutableList<Predicate> predicates = predicatesMap.get(columnName);
    if (predicates.size() != 1) {
      return false;
    }
    return predicates.get(0).operator == Operator.EQUAL_TO;
  }

  public static com.scalar.db.api.TableMetadata getTableMetadata(
      TableMetadataManager tableMetadataManager, String namespaceName, String tableName) {
    try {
      com.scalar.db.api.TableMetadata metadata =
          tableMetadataManager.getTableMetadata(namespaceName, tableName);
      if (metadata == null) {
        throw new TableNotFoundException(namespaceName, tableName);
      }
      return metadata;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get a table metadata", e);
    }
  }
}
