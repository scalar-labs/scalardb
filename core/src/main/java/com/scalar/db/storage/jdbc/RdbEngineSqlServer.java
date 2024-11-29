package com.scalar.db.storage.jdbc;

import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.MergeQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithTop;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import microsoft.sql.DateTimeOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEngineSqlServer implements RdbEngineStrategy {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineSqlServer.class);

  @Override
  public String[] createSchemaSqls(String fullSchema) {
    return new String[] {"CREATE SCHEMA " + enclose(fullSchema)};
  }

  @Override
  public String[] createSchemaIfNotExistsSqls(String fullSchema) {
    return createSchemaSqls(fullSchema);
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
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    // do nothing
    return new String[0];
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql;
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // 2714: There is already an object named '%.*ls' in the database.
    return e.getErrorCode() == 2714;
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
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    // SQLServer does not require changes in column data types when making indices.
    throw new AssertionError();
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT TOP 1 1 FROM " + fullTableName;
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName) + " ON " + encloseFullTableName(schema, table);
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // 2714: There is already an object named '%.*ls' in the database.
    return e.getErrorCode() == 2714;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // 23000: Integrity constraint violation
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // 208: Invalid object name '%.*ls'.
    return e.getErrorCode() == 208;
  }

  @Override
  public boolean isConflict(SQLException e) {
    // 1205: Transaction (Process ID %d) was deadlocked on %.*ls resources with another process and
    // has been chosen as the deadlock victim. Rerun the transaction.
    return e.getErrorCode() == 1205;
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    // https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors-1000-to-1999?view=sql-server-ver16
    // Error code: 1913
    // Message: The operation failed because an index or statistics with name '%.*ls' already exists
    // on %S_MSG '%.*ls'.
    return e.getErrorCode() == 1913;
  }

  @Override
  public String enclose(String name) {
    return "[" + name + "]";
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithTop(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new MergeQuery(builder);
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "VARBINARY(8000)";
      case BOOLEAN:
        return "BIT";
      case DOUBLE:
        return "FLOAT";
      case FLOAT:
        return "FLOAT(24)";
      case INT:
        return "INT";
      case TEXT:
        return "VARCHAR(8000)";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME(6)";
      case TIMESTAMP:
        return "DATETIME2(3)";
      case TIMESTAMPTZ:
        return "DATETIMEOFFSET(3)";
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    // SQL Server does not require any change in column data types when making indices.
    return null;
  }

  @Override
  public DataType getDataTypeForScalarDb(
      JDBCType type, String typeName, int columnSize, int digits, String columnDescription) {
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
        return DataType.INT;
      case BIGINT:
        logger.warn(
            "Data type that may be smaller than that of underlying database is assigned: {} (SQL Server {} to ScalarDB BIGINT)",
            columnDescription,
            typeName);
        return DataType.BIGINT;
      case REAL:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case NCHAR:
      case VARCHAR:
      case NVARCHAR:
        if (typeName.equalsIgnoreCase("uniqueidentifier")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to TEXT)",
            columnDescription,
            typeName);
        return DataType.TEXT;
      case LONGVARCHAR:
      case LONGNVARCHAR:
        if (typeName.equalsIgnoreCase("xml")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
        if (!typeName.equalsIgnoreCase("binary") && !typeName.equalsIgnoreCase("varbinary")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        if (columnSize < Integer.MAX_VALUE) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} ({} to BLOB)",
              columnDescription,
              typeName);
        }
        return DataType.BLOB;
      case LONGVARBINARY:
        return DataType.BLOB;
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
        return Types.TIMESTAMP;
      case TIMESTAMPTZ:
        return Types.TIMESTAMP_WITH_TIMEZONE;
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
    return value ? "1" : "0";
  }

  @Override
  public Driver getDriver() {
    return new com.microsoft.sqlserver.jdbc.SQLServerDriver();
  }

  @Override
  public String getPattern(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    String pattern = likeExpression.getTextValue();
    if (escape.isEmpty()) {
      // Even if users do not want to use escape character in ScalarDB (i.e., specifying "" for
      // escape character rather than omitting it), we need to add an implicit escape character to
      // escape SQL server specific escape characters ("[" and "]") because it always works even
      // without specifying the escape clause. We use "\" as the implicit escape character, so we
      // also need to escape it to achieve the user's original intention (i.e., no escape).
      return pattern.replaceAll("[\\[\\]\\\\]", "\\\\$0");
    } else {
      // Only escape SQL server specific escape characters ("[" and "]") with the specified escape.
      return pattern.replaceAll(
          "[\\[\\]]", String.format("%s$0", escape.equals("\\") ? "\\\\" : escape));
    }
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? "\\" : escape;
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    return createIndexSql;
  }

  @Override
  public Object encodeDate(DateColumn column) {
    assert column.getDateValue() != null;
    return column.getDateValue().format(DateTimeFormatter.BASIC_ISO_DATE);
  }

  @Override
  public Object encodeTimestamp(TimestampColumn column) {
    assert column.getTimestampValue() != null;
    return column.getTimestampValue().format(DateTimeFormatter.ISO_DATE_TIME);
  }

  @Override
  public Object encodeTimestampTZ(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;
    //    return DateTimeOffset.valueOf(column.getTimestampTZValue().atOffset(ZoneOffset.UTC));
    //    return DateTimeFormatter.ISO_INSTANT.format(column.getTimestampTZValue());
    return DateTimeOffset.valueOf(Timestamp.from(column.getTimestampTZValue()), 0);
  }
}
