package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.SQLException;

/**
 * An interface to hide the difference between underlying JDBC SQL engines in SQL dialects, error
 * codes, and so on. It's NOT responsible for actually connecting to underlying engines.
 */
public interface RdbEngineStrategy {

  boolean isDuplicateTableError(SQLException e);

  boolean isDuplicateKeyError(SQLException e);

  boolean isUndefinedTableError(SQLException e);

  boolean isConflictError(SQLException e);

  String getDataTypeForEngine(DataType dataType);

  String getDataTypeForKey(DataType dataType);

  int getSqlTypes(DataType dataType);

  String getTextType(int charLength);

  String computeBooleanValue(boolean value);

  String[] createNamespaceSqls(String fullNamespace);

  String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDescClusteringOrder, String schema, String table, TableMetadata metadata);

  String tryAddIfNotExistsToCreateTableSql(String createTableSql);

  String[] createMetadataSchemaIfNotExistsSql(String metadataSchema);

  boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e);

  String deleteMetadataSchemaSql(String metadataSchema);

  String dropNamespaceSql(String namespace);

  void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException;

  String namespaceExistsStatement();

  String alterColumnTypeSql(String namespace, String table, String columnName, String columnType);

  String tableExistsInternalTableCheckSql(String fullTableName);

  String dropIndexSql(String schema, String table, String indexName);

  /**
   * Enclose the target (schema, table or column) to use reserved words and special characters.
   *
   * @param name The target name to enclose
   * @return An enclosed string of the target name
   */
  String enclose(String name);

  default String encloseFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }

  SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit);

  UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder);

  Driver getDriverClass();
}
