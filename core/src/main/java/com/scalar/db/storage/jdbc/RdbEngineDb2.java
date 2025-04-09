package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.MergeIntoQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithFetchFirstNRowsOnly;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

class RdbEngineDb2 extends AbstractRdbEngine {
  private final RdbEngineTimeTypeDb2 timeTypeEngine;
  // TODO Create configuration for key column size
  private final String keyColumnSize = "128";

  public RdbEngineDb2(JdbcConfig config) {
    timeTypeEngine = new RdbEngineTimeTypeDb2(config);
  }

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
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + "))";
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    ArrayList<String> sqls = new ArrayList<>();

    if (hasDifferentClusteringOrders) {
      // Create a unique index for the clustering orders only when both ASC and DESC are contained
      // in the clustering keys. If all the clustering key orders are DESC, the PRIMARY KEY index
      // can be used.
      sqls.add(
          "CREATE UNIQUE INDEX "
              + enclose(getFullTableName(schema, table) + "_clustering_order_idx")
              + " ON "
              + encloseFullTableName(schema, table)
              + " ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                      metadata.getClusteringKeyNames().stream()
                          .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                  .collect(Collectors.joining(","))
              + ")");
    }
    return sqls.toArray(new String[0]);
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // SQL error code -601: Name already in used
    return e.getErrorCode() == -601;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    return dropNamespaceSql(metadataSchema);
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    return "DROP SCHEMA " + enclose(namespace) + " RESTRICT";
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException {
    throw new ExecutionException("Dropping the schema failed: " + namespace, e);
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    return "ALTER TABLE "
        + encloseFullTableName(namespace, table)
        + " ALTER COLUMN "
        + enclose(columnName)
        + " SET DATA TYPE "
        + columnType;
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " FETCH FIRST 1 ROW ONLY";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName);
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    // Even though the "create index if exists ..." syntax does exist,
    // only a warning is raised when the index already exists but no error is thrown
    // so we return false in any case
    return false;
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    return createIndexSql;
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithFetchFirstNRowsOnly(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return MergeIntoQuery.createForDb2(builder);
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // "if not exists" syntax exists so we return false in any case
    return false;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // SQL error code -803:  AN INSERTED OR UPDATED VALUE IS INVALID BECAUSE THE INDEX IN INDEX
    // SPACE
    // indexspace-name CONSTRAINS COLUMNS OF THE TABLE SO NO TWO ROWS CAN CONTAIN DUPLICATE VALUES
    // IN THOSE COLUMNS. RID OF EXISTING ROW IS X record-id
    return e.getErrorCode() == -803;
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // SQL error code -204: name IS AN UNDEFINED NAME
    return e.getErrorCode() == -204;
  }

  @Override
  public boolean isConflict(SQLException e) {
    // Sql error code -911: THE CURRENT UNIT OF WORK HAS BEEN ROLLED BACK DUE TO DEADLOCK OR TIMEOUT
    return e.getErrorCode() == -911;
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        // TODO Too big ?
        return "VARBINARY(32672)";
      case BOOLEAN:
        return "BOOLEAN";
      case FLOAT:
        return "REAL";
      case DOUBLE:
        return "DOUBLE";
      case INT:
        return "INTEGER";
      case TEXT:
        // TODO Too big ?
        return "VARCHAR(32672)";
      case DATE:
        return "TIMESTAMP(0)";
      case TIME:
        return "TIMESTAMP(6)";
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return "TIMESTAMP(3)";
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return "VARCHAR(" + keyColumnSize + ") NOT NULL";
      case BLOB:
        return "VARBINARY(" + keyColumnSize + ") NOT NULL";
      default:
        return getDataTypeForEngine(dataType) + " NOT NULL";
    }
  }

  @Override
  public String getDataTypeForSecondaryIndex(DataType dataType) {
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
    // TODO Check for import table
    switch (type) {
      case BIT:
        if (columnSize != 1) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_WITH_SIZE_NOT_SUPPORTED.buildMessage(
                  typeName, columnSize, columnDescription));
        }
        return DataType.BOOLEAN;
      case SMALLINT:
        return DataType.INT;
      case INTEGER:
        return DataType.INT;
      case BIGINT:
        return DataType.BIGINT;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case DECIMAL:
      case NUMERIC:
        if (digits == 0) {
          return DataType.INT;
        }
        return DataType.DOUBLE;
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        return DataType.BLOB;
      case DATE:
        return DataType.DATE;
      case TIME:
        return DataType.TIME;
      case TIMESTAMP:
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
      case BIGINT:
        return Types.BIGINT;
      case BLOB:
        return Types.BLOB;
      case BOOLEAN:
        return Types.BOOLEAN;
      case DOUBLE:
        return Types.DOUBLE;
      case FLOAT:
        return Types.REAL;
      case INT:
        return Types.INTEGER;
      case TEXT:
        return Types.VARCHAR;
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return Types.TIMESTAMP;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getTextType(int charLength, boolean isKey) {
    return "VARCHAR(" + charLength + ")" + (isKey ? " NOT NULL" : "");
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "1" : "0";
  }

  @Override
  public Driver getDriver() {
    return new com.ibm.db2.jcc.DB2Driver();
  }

  @Override
  public DateColumn parseDateColumn(ResultSet resultSet, String columnName) throws SQLException {
    String timestampStr = resultSet.getString(columnName);
    if (timestampStr == null) {
      return DateColumn.ofNull(columnName);
    } else {
      LocalDate timestamp =
          RdbEngineTimeTypeDb2.TIMESTAMP_FORMATTER.parse(timestampStr, LocalDate::from);
      return DateColumn.of(columnName, timestamp);
    }
  }

  @Override
  public TimeColumn parseTimeColumn(ResultSet resultSet, String columnName) throws SQLException {
    // TODO Investigate if normal behavior that NPE is thrown when the value is null
    try {
      LocalTime time = resultSet.getObject(columnName, LocalDateTime.class).toLocalTime();
      return TimeColumn.of(columnName, time);
    } catch (NullPointerException e) {
      if (resultSet.wasNull()) {
        return TimeColumn.ofNull(columnName);
      } else {
        throw e;
      }
    }
  }

  @Override
  public TimestampColumn parseTimestampColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    String timestampStr = resultSet.getString(columnName);
    if (timestampStr == null) {
      return TimestampColumn.ofNull(columnName);
    } else {
      LocalDateTime timestamp =
          RdbEngineTimeTypeDb2.TIMESTAMP_FORMATTER.parse(timestampStr, LocalDateTime::from);
      return TimestampColumn.of(columnName, timestamp);
    }
  }

  @Override
  public TimestampTZColumn parseTimestampTZColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    String timestampStr = resultSet.getString(columnName);
    if (timestampStr == null) {
      return TimestampTZColumn.ofNull(columnName);
    } else {
      Instant timestampTZ =
          RdbEngineTimeTypeDb2.TIMESTAMP_FORMATTER.parse(timestampStr, Instant::from);
      return TimestampTZColumn.of(columnName, timestampTZ);
    }
  }

  @Override
  public RdbEngineTimeTypeStrategy<String, LocalDateTime, String, String> getTimeTypeStrategy() {
    return timeTypeEngine;
  }

  @Override
  public Map<String, String> getConnectionProperties() {
    ImmutableMap.Builder<String, String> props = new ImmutableMap.Builder<>();
    // With this property set to true, Db2 will return a textual description of the error when
    // calling JDBC `SQLException.getMessage` instead of a message only containing error codes
    props.put("retrieveMessagesFromServerOnGetMessage", "true");

    // With this property set to true, db2 will not make adjustment:
    // - for daylight saving time
    // - for the problematic period of October 5, 1582, through October 14, 1582 because the Julian
    // to Gregorian calendar conversion
    // Cf.
    // https://www.ibm.com/docs/en/db2/12.1.0?topic=dttmddtija-date-time-timestamp-values-that-can-cause-problems-in-jdbc-sqlj-applications
    props.put("sqljAvoidTimeStampConversion", "true");

    return props.build();
  }

  @Override
  public String truncateTableSql(String namespace, String table) {
    return "TRUNCATE TABLE " + encloseFullTableName(namespace, table) + " IMMEDIATE";
  }
}
