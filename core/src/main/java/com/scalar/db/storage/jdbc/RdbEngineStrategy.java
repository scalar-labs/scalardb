package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * An interface to hide the difference between underlying JDBC SQL engines in SQL dialects, error codes, and so on.
 * It's NOT responsible for actually connecting to underlying engines.
 */
public interface RdbEngineStrategy {

  boolean isDuplicateUserError(SQLException e);

  boolean isDuplicateSchemaError(SQLException e);

  boolean isDuplicateTableError(SQLException e);

  boolean isDuplicateKeyError(SQLException e);

  boolean isUndefinedTableError(SQLException e);
  /** Serialization error or deadlock found. */
  boolean isConflictError(SQLException e);

  String getDataTypeForEngine(DataType dataType);

  String getDataTypeForKey(DataType dataType);

  int getSqlTypes(DataType dataType);

  String getTextType(int charLength);

  String computeBooleanValue(boolean value);

  String[] createNamespaceExecuteSqls(String fullNamespace);

  String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDescClusteringOrder,
      String schema,
      String table,
      TableMetadata metadata);

  String tryAddIfNotExistsToCreateTableSql(String createTableSql);

  String[] createMetadataSchemaIfNotExistsSql(String metadataSchema);

  boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e);

  String deleteMetadataSchemaSql(String metadataSchema);

  String dropNamespaceSql(String namespace);

  void dropNamespaceTranslateSQLException(SQLException e, String namespace) throws ExecutionException;

  String namespaceExistsStatement();

  void alterColumnType(
      Connection connection, String namespace, String table, String columnName, String columnType)
      throws SQLException;

  void tableExistsInternalExecuteTableCheck(Connection connection, String fullTableName)
      throws SQLException;

  void dropIndexExecute(Connection connection, String schema, String table, String indexName)
      throws SQLException;

  /**
   * Enclose the target (schema, table or column) to use reserved words and special characters.
   *
   * @param name The target name to enclose
   * @return An enclosed string of the target name
   */
  String enclose(String name);

  String encloseFullTableName(String schema, String table);

  SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit);

  UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder);
}
