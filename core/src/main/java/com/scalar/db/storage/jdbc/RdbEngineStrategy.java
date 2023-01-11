package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

public interface RdbEngineStrategy {

  abstract boolean isDuplicateUserError(SQLException e);

  abstract boolean isDuplicateSchemaError(SQLException e);

  abstract boolean isDuplicateTableError(SQLException e);

  abstract boolean isDuplicateKeyError(SQLException e);

  abstract boolean isUndefinedTableError(SQLException e);
  /** Serialization error or deadlock found. */
  public abstract boolean isConflictError(SQLException e);

  public abstract RdbEngine getRdbEngine();

  abstract String getDataTypeForEngine(DataType dataType);

  abstract String getDataTypeForKey(DataType dataType);

  abstract String getTextType(int charLength);

  abstract String computeBooleanValue(boolean value);

  abstract void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException;

  abstract String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  abstract void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException;

  abstract void createMetadataTableIfNotExistsExecute(
      Connection connection, String createTableStatement) throws SQLException;

  abstract void createMetadataSchemaIfNotExists(Connection connection, String metadataSchema)
      throws SQLException;

  abstract void deleteMetadataSchema(Connection connection, String metadataSchema)
      throws SQLException;

  abstract void dropNamespace(BasicDataSource dataSource, String namespace)
      throws ExecutionException;

  abstract String namespaceExistsStatement();

  abstract void alterColumnType(
      Connection connection, String namespace, String table, String columnName, String columnType)
      throws SQLException;

  abstract void tableExistsInternalExecuteTableCheck(Connection connection, String fullTableName)
      throws SQLException;

  abstract void dropIndexExecute(
      Connection connection, String schema, String table, String indexName) throws SQLException;

  /**
   * Enclose the target (schema, table or column) to use reserved words and special characters.
   *
   * @param name The target name to enclose
   * @return An enclosed string of the target name
   */
  public abstract String enclose(String name);

  public abstract String encloseFullTableName(String schema, String table);
}
