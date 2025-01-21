package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.InsertOnDuplicateKeyUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEngineMysql extends AbstractRdbEngine {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineMysql.class);
  private final String keyColumnSize;
  private final RdbEngineTimeTypeMysql timeTypeEngine;

  RdbEngineMysql(JdbcConfig config) {
    keyColumnSize = String.valueOf(config.getMysqlVariableKeyColumnSize());
    timeTypeEngine = new RdbEngineTimeTypeMysql();
  }

  @VisibleForTesting
  RdbEngineMysql() {
    keyColumnSize = String.valueOf(JdbcConfig.DEFAULT_VARIABLE_KEY_COLUMN_SIZE);
    timeTypeEngine = new RdbEngineTimeTypeMysql();
  }

  @Override
  public String[] createNamespaceSqls(String fullNamespace) {
    return new String[] {"CREATE SCHEMA " + fullNamespace};
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    if (hasDescClusteringOrder) {
      return "PRIMARY KEY ("
          + Stream.concat(
                  metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                  metadata.getClusteringKeyNames().stream()
                      .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
              .collect(Collectors.joining(","))
          + "))";
    } else {
      return "PRIMARY KEY ("
          + Stream.concat(
                  metadata.getPartitionKeyNames().stream(),
                  metadata.getClusteringKeyNames().stream())
              .map(this::enclose)
              .collect(Collectors.joining(","))
          + "))";
    }
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
    return new String[] {"CREATE SCHEMA IF NOT EXISTS " + enclose(metadataSchema)};
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    return false;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    return "DROP SCHEMA " + enclose(metadataSchema);
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    return "DROP SCHEMA " + enclose(namespace);
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException {
    throw new ExecutionException("Dropping the schema failed: " + namespace, e);
  }

  @Override
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM "
        + encloseFullTableName("information_schema", "schemata")
        + " WHERE "
        + enclose("schema_name")
        + " = ?";
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    return "ALTER TABLE "
        + encloseFullTableName(namespace, table)
        + " MODIFY"
        + enclose(columnName)
        + " "
        + columnType;
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " LIMIT 1";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName) + " ON " + encloseFullTableName(schema, table);
  }

  @Override
  public String enclose(String name) {
    return "`" + name + "`";
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithLimitQuery(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new InsertOnDuplicateKeyUpdateQuery(builder);
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // Error number: 1050; Symbol: ER_TABLE_EXISTS_ERROR; SQLSTATE: 42S01
    // Message: Table '%s' already exists
    return e.getErrorCode() == 1050;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // Error number: 1022; Symbol: ER_DUP_KEY; SQLSTATE: 23000
    // Message: Can't write; duplicate key in table '%s'
    // etc... See: <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // Error number: 1049; Symbol: ER_BAD_DB_ERROR; SQLSTATE: 42000
    // Message: Unknown database '%s'

    // Error number: 1146; Symbol: ER_NO_SUCH_TABLE; SQLSTATE: 42S02
    // Message: Table '%s.%s' doesn't exist

    return e.getErrorCode() == 1049 || e.getErrorCode() == 1146;
  }

  @Override
  public boolean isConflict(SQLException e) {
    // Error number: 1213; Symbol: ER_LOCK_DEADLOCK; SQLSTATE: 40001
    // Message: Deadlock found when trying to get lock; try restarting transaction

    // Error number: 1205; Symbol: ER_LOCK_WAIT_TIMEOUT; SQLSTATE: HY000
    // Message: Lock wait timeout exceeded; try restarting transaction

    return e.getErrorCode() == 1213 || e.getErrorCode() == 1205;
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "LONGBLOB";
      case BOOLEAN:
        return "BOOLEAN";
      case DOUBLE:
        return "DOUBLE";
      case FLOAT:
        return "REAL";
      case INT:
        return "INT";
      case TEXT:
        return "LONGTEXT";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME(6)";
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return "DATETIME(3)";
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return "VARCHAR(" + keyColumnSize + ")";
      case BLOB:
        return "VARBINARY(" + keyColumnSize + ")";
      default:
        return null;
    }
  }

  @Override
  DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType) {
    switch (type) {
      case BIT:
        if (columnSize != 1) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_WITH_SIZE_NOT_SUPPORTED.buildMessage(
                  typeName, columnSize, columnDescription));
        }
        return DataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to INT)",
            columnDescription,
            typeName);
        return DataType.INT;
      case INTEGER:
        if (typeName.toUpperCase().endsWith("UNSIGNED")) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} ({} to BIGINT)",
              columnDescription,
              typeName);
          return DataType.BIGINT;
        }
        return DataType.INT;
      case BIGINT:
        if (typeName.toUpperCase().endsWith("UNSIGNED")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        logger.warn(
            "Data type that may be smaller than that of underlying database is assigned: {} (MySQL {} to ScalarDB BIGINT)",
            columnDescription,
            typeName);
        return DataType.BIGINT;
      case REAL:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
        if (!typeName.toUpperCase().endsWith("CHAR") && !typeName.toUpperCase().endsWith("TEXT")) {
          // to exclude ENUM, SET, JSON, etc.
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        if (!typeName.equalsIgnoreCase("LONGTEXT")) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} ({} to TEXT)",
              columnDescription,
              typeName);
        }
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        if (!typeName.toUpperCase().endsWith("BINARY")
            && !typeName.toUpperCase().endsWith("BLOB")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        if (!typeName.equalsIgnoreCase("LONGBLOB")) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} ({} to BLOB)",
              columnDescription,
              typeName);
        }
        return DataType.BLOB;
      case DATE:
        if (typeName.equalsIgnoreCase("YEAR")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.DATE;
      case TIME:
        return DataType.TIME;
        // Both MySQL TIMESTAMP and DATETIME data types are mapped to the TIMESTAMP JDBC type
      case TIMESTAMP:
        if (overrideDataType == DataType.TIMESTAMPTZ || typeName.equalsIgnoreCase("TIMESTAMP")) {
          return DataType.TIMESTAMPTZ;
        }
        return DataType.TIMESTAMP;
      default:
        throw new IllegalArgumentException(
            CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                typeName, columnDescription));
    }
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
      case DATE:
        return Types.DATE;
      case TIME:
        return Types.TIME;
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return Types.TIMESTAMP;

      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getTextType(int charLength) {
    return String.format("VARCHAR(%s)", charLength);
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "true" : "false";
  }

  @Override
  public Driver getDriver() {
    try {
      return new com.mysql.cj.jdbc.Driver();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getPattern(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    String pattern = likeExpression.getTextValue();
    if (escape.isEmpty()) {
      // MySQL accepts an empty escape character to disable the escape function, but MariaDB ignores
      // the empty escape character (i.e., default escape character "\" is used). To handle both
      // databases with the same SQL statement, we make the escape character disabled by internally
      // double-escaping with the implicit escape character "\".
      return pattern.replace("\\", "\\\\");
    } else {
      return pattern;
    }
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? "\\" : escape;
  }

  @Nullable
  @Override
  public String getCatalogName(String namespace) {
    return namespace;
  }

  @Nullable
  @Override
  public String getSchemaName(String namespace) {
    // This can be null. However, we return the namespace from this method just in case since users
    // might be able to set `databaseTerm` property to `SCHEMA` so that a return value from this
    // method is used for filtering.
    return namespace;
  }

  @Override
  public TimestampTZColumn parseTimestampTZColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    LocalDateTime localDateTime = resultSet.getObject(columnName, LocalDateTime.class);
    if (localDateTime == null) {
      return TimestampTZColumn.ofNull(columnName);
    } else {
      return TimestampTZColumn.of(columnName, localDateTime.toInstant(ZoneOffset.UTC));
    }
  }

  @Override
  public RdbEngineTimeTypeStrategy<LocalDate, LocalTime, LocalDateTime, LocalDateTime>
      getTimeTypeStrategy() {
    return timeTypeEngine;
  }
}
