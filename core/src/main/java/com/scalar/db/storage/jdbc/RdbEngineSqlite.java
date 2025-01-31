package com.scalar.db.storage.jdbc;

import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

/**
 * A RdnEngineStrategy implementation for SQLite.
 *
 * <p>Namespace: Added to table prefix like `${namespace}${NAMESPACE_SEPARATOR}${tableName}`.
 *
 * <p>Error handling: SQLite has limited variety of error codes compared to other JDBC databases. We
 * need to heavily rely on error messages, which may be changed in SQLite implementation in the
 * future. Since <a
 * href="https://github.com/xerial/sqlite-jdbc#sqlite-jdbc-driver">xerial/sqlite-jdbc contains a
 * native SQLite library in JAR</a>, we should assure the real error messages in
 * RdbEngineStrategyTest.
 */
public class RdbEngineSqlite extends AbstractRdbEngine {
  private static final String NAMESPACE_SEPARATOR = "$";
  private final RdbEngineTimeTypeSqlite timeTypeEngine;

  public RdbEngineSqlite() {
    timeTypeEngine = new RdbEngineTimeTypeSqlite();
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // Error code: SQLITE_ERROR (1)
    // Message: SQL error or missing database (table XXX already exists)

    return e.getErrorCode() == 1
        && e.getMessage().contains("(table")
        && e.getMessage().endsWith("already exists)");
  }

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // Error code: SQLITE_CONSTRAINT_PRIMARYKEY (1555)

    // Error code: SQLITE_CONSTRAINT_UNIQUE (2067)

    return ((SQLiteException) e).getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY
        || ((SQLiteException) e).getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE;
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // Error code: SQLITE_ERROR (1)
    // Message: SQL error or missing database (no such table: XXX)

    return e.getErrorCode() == 1 && e.getMessage().contains("no such table:");
  }

  @Override
  public boolean isConflict(SQLException e) {
    // Error code: SQLITE_BUSY (5)
    // Message: The database file is locked (database is locked)

    // Error code: SQLITE_BUSY (6)
    // Message: A table in the database is locked (database table is locked)

    return e.getErrorCode() == 5 || e.getErrorCode() == 6;
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INT:
      case DATE:
        return "INT";
      case BIGINT:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
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
      case DATE:
        return Types.INTEGER;
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
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
  public DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      DataType overrideDataType) {
    throw new AssertionError("SQLite is not supported");
  }

  @Override
  public boolean isValidTableName(String tableName) {
    return !tableName.contains(NAMESPACE_SEPARATOR);
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
      boolean hasDifferentClusteringOrders, String schema, String table, TableMetadata metadata) {
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
    // Do nothing. Metadata schema is just a prefix to the metadata table in the SQLite
    // implementation.
    return null;
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    // Do nothing. Namespace is just a table prefix in the SQLite implementation.
    return null;
  }

  @Override
  public String truncateTableSql(String namespace, String table) {
    // SQLite does not support TRUNCATE TABLE statement.
    return "DELETE FROM " + encloseFullTableName(namespace, table);
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace) {
    throw new AssertionError("DropNamespace never happen in SQLite implementation");
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
    return namespace + NAMESPACE_SEPARATOR + "%";
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    throw new AssertionError(
        "SQLite does not require changes in column data types when making indices");
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " LIMIT 1";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName);
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String encloseFullTableName(String schema, String table) {
    return enclose(schema + NAMESPACE_SEPARATOR + table);
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithLimitQuery(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new InsertOnConflictDoUpdateQuery(builder);
  }

  @Override
  public Driver getDriver() {
    return new org.sqlite.JDBC();
  }

  @Override
  public boolean isImportable() {
    return false;
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? null : escape;
  }

  @Override
  public DateColumn parseDateColumn(ResultSet resultSet, String columnName) throws SQLException {
    return DateColumn.of(
        columnName, TimeRelatedColumnEncodingUtils.decodeDate(resultSet.getInt(columnName)));
  }

  @Override
  public TimeColumn parseTimeColumn(ResultSet resultSet, String columnName) throws SQLException {
    return TimeColumn.of(
        columnName, TimeRelatedColumnEncodingUtils.decodeTime(resultSet.getLong(columnName)));
  }

  @Override
  public TimestampColumn parseTimestampColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    return TimestampColumn.of(
        columnName, TimeRelatedColumnEncodingUtils.decodeTimestamp(resultSet.getLong(columnName)));
  }

  @Override
  public TimestampTZColumn parseTimestampTZColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    return TimestampTZColumn.of(
        columnName,
        TimeRelatedColumnEncodingUtils.decodeTimestampTZ(resultSet.getLong(columnName)));
  }

  @Override
  public RdbEngineTimeTypeStrategy<Integer, Long, Long, Long> getTimeTypeStrategy() {
    return timeTypeEngine;
  }
}
