package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

public interface RdbEngineStrategy {

  boolean isDuplicateUserError(SQLException e);

  boolean isDuplicateSchemaError(SQLException e);

  boolean isDuplicateTableError(SQLException e);

  boolean isDuplicateKeyError(SQLException e);

  boolean isUndefinedTableError(SQLException e);
  /** Serialization error or deadlock found. */
  boolean isConflictError(SQLException e);

  RdbEngine getRdbEngine();

  String getDataTypeForEngine(DataType dataType);

  String getDataTypeForKey(DataType dataType);

  String getTextType(int charLength);

  String computeBooleanValue(boolean value);

  void createNamespaceExecute(Connection connection, String fullNamespace) throws SQLException;

  String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException;

  void createMetadataTableIfNotExistsExecute(Connection connection, String createTableStatement)
      throws SQLException;

  void createMetadataSchemaIfNotExists(Connection connection, String metadataSchema)
      throws SQLException;

  void deleteMetadataSchema(Connection connection, String metadataSchema) throws SQLException;

  void dropNamespace(BasicDataSource dataSource, String namespace) throws ExecutionException;

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

  default String encloseFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }
}
