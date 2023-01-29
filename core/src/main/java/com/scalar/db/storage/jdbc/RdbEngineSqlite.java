package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;

import java.sql.SQLException;

/**
 * Namespace: Added to table prefix like `\<namespace\>_\<table name\>`.
 */
public class RdbEngineSqlite implements RdbEngineStrategy {
  @Override
  public boolean isDuplicateUserError(SQLException e) {
    return false;
  }

  @Override
  public boolean isDuplicateSchemaError(SQLException e) {
    return false;
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    return false;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    return false;
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    return false;
  }

  @Override
  public boolean isConflictError(SQLException e) {
    return false;
  }

  @Override
  public String getDataTypeForEngine(DataType dataType) {
    return null;
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    return null;
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    return 0;
  }

  @Override
  public String getTextType(int charLength) {
    return null;
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return null;
  }

  @Override
  public String[] createNamespaceExecuteSqls(String fullNamespace) {
    // In SQLite storage, namespace will be added to table names as prefix along with underscore
    // separator.
    return new String[0];
  }

  @Override
  public String createTableInternalPrimaryKeyClause(boolean hasDescClusteringOrder, TableMetadata metadata) {
    return null;
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(boolean hasDescClusteringOrder, String schema, String table, TableMetadata metadata) {
    return new String[0];
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return null;
  }

  @Override
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    return new String[0];
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    return false;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    return null;
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    return null;
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace) throws ExecutionException {

  }

  @Override
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM sqlite_master WHERE "
               + enclose("type")
               + " = \"table\" AND "
               + enclose("tbl_name")
               + " LIKE ?";
  }

  @Override
  public String alterColumnTypeSql(String namespace, String table, String columnName, String columnType) {
    return null;
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return null;
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return null;
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String encloseFullTableName(String schema, String table) {
    return schema + "_" + table;
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return null;
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return null;
  }
}
