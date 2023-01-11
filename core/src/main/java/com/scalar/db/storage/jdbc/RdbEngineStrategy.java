package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

public abstract class RdbEngineStrategy {

  abstract boolean isDuplicateUserError(SQLException e);

  abstract boolean isDuplicateSchemaError(SQLException e);

  abstract boolean isDuplicateTableError(SQLException e);

  abstract boolean isDuplicateKeyError(SQLException e);

  abstract boolean isUndefinedTableError(SQLException e);
  /** Serialization error or deadlock found. */
  public abstract boolean isConflictError(SQLException e);

  public abstract RdbEngine getRdbEngine();

  protected abstract String getDataTypeForEngine(DataType dataType);

  protected abstract String getDataTypeForKey(DataType dataType);

  abstract String getTextType(int charLength);

  abstract String computeBooleanValue(boolean value);

  protected abstract void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException;

  protected abstract String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  protected abstract void createTableInternalExecuteAfterCreateTable(
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

  protected String enclose(String name) {
    return QueryUtils.enclose(name, getRdbEngine());
  }

  protected String encloseFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }
}
