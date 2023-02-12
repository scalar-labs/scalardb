package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Namespace: Added to table prefix like `${namespace}_${tableName}`. */
public class RdbEngineSqlite implements RdbEngineStrategy {
  @Override
  public boolean isDuplicateTableError(SQLException e) {
    return e.getErrorCode() == 1
        && e.getMessage().contains("table")
        && e.getMessage().endsWith("already exists");
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // Error code: SQLITE_ERROR (1)
    // Message: SQL error or missing database (no such table: XXX)

    // NOTE: SQLite has limited variety of error codes.
    // We might have to check the error message in unit tests.
    // xerial/sqlite-jdbc contains a native SQLite library in JAR,
    // (<https://github.com/xerial/sqlite-jdbc#sqlite-jdbc-driver>)
    // so unit testing would assure the error message assertions.

    return e.getErrorCode() == 1 && e.getMessage().contains("no such table:");
  }

  @Override
  public boolean isConflictError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INT:
        return "INT";
      case BIGINT:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case TEXT:
        return "TEXT";
      case BLOB:
        return "BLOB";
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    return null;
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INT:
        return Types.INTEGER;
      case BIGINT:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case TEXT:
        return Types.VARCHAR;
      case BLOB:
        return Types.BLOB;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getTextType(int charLength) {
    return "TEXT";
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "TRUE" : "FALSE";
  }

  @Override
  public String[] createNamespaceSqls(String fullNamespace) {
    // In SQLite storage, namespace will be added to table names as prefix along with underscore
    // separator.
    return new String[0];
  }

  /**
   * @param hasDescClusteringOrder Ignored. SQLite cannot handle key order.
   * @see <a href="https://www.sqlite.org/syntax/table-constraint.html">table-constraint</a>
   */
  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + "))";
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDescClusteringOrder, String schema, String table, TableMetadata metadata) {
    // do nothing
    return new String[] {};
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
  }

  @Override
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    // Do nothing. Namespace is just a table prefix in the SQLite implementation.
    return new String[0];
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // Namespace is never created
    return false;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    // Do nothing. Namespace is just a table prefix in the SQLite implementation.
    return null;
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    // Do nothing. Namespace is just a table prefix in the SQLite implementation.
    return null;
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException {
    throw new AssertionError("dropNamespace never happen in SQLite implementation");
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
  public String namespaceExistsPlaceholder(String namespace) {
    return namespace + "_%";
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    throw new AssertionError(
        "SQLite does not require changes in column data types when making indices.");
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " LIMIT 1";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    // TODO SQLite cannot scope an index name to a table. Consider adding <namespace>_ prefix to
    // index names.
    return "DROP INDEX " + enclose(indexName);
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String encloseFullTableName(String schema, String table) {
    return enclose(schema + "_" + table);
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithLimitQuery(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new InsertOnConflictDoUpdateQuery(builder);
  }
}
