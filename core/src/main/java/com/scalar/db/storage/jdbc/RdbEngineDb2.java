package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.ibm.db2.jcc.DB2BaseDataSource;
import com.scalar.db.api.LikeExpression;
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
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEngineDb2 extends AbstractRdbEngine {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineMysql.class);
  private final RdbEngineTimeTypeDb2 timeTypeEngine;
  private final String keyColumnSize;

  public RdbEngineDb2(JdbcConfig config) {
    timeTypeEngine = new RdbEngineTimeTypeDb2(config);
    keyColumnSize = String.valueOf(config.getDb2VariableKeyColumnSize());
  }

  @VisibleForTesting
  RdbEngineDb2() {
    timeTypeEngine = null;
    keyColumnSize = String.valueOf(JdbcConfig.DEFAULT_VARIABLE_KEY_COLUMN_SIZE);
  }

  @Override
  public Driver getDriver() {
    return new com.ibm.db2.jcc.DB2Driver();
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "VARBINARY(32672)";
      case BOOLEAN:
        return "BOOLEAN";
      case FLOAT:
        return "REAL";
      case DOUBLE:
        return "DOUBLE";
      case INT:
        return "INT";
      case TEXT:
        return "VARCHAR(32672)";
      case DATE:
        return "DATE";
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
  DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType) {
    switch (type) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case SMALLINT:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to INT)",
            columnDescription,
            typeName);
        return DataType.INT;
      case INTEGER:
        return DataType.INT;
      case BIGINT:
        return DataType.BIGINT;
      case REAL:
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case VARCHAR:
      case CLOB:
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
      case BLOB:
        return DataType.BLOB;
      case DATE:
        return DataType.DATE;
      case TIME:
        return DataType.TIME;
      case TIMESTAMP:
        if (overrideDataType == DataType.TIME) {
          return DataType.TIME;
        }
        if (overrideDataType == DataType.TIMESTAMPTZ) {
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
        return Types.DATE;
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return Types.TIMESTAMP;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String[] createNamespaceSqls(String fullNamespace) {
    return new String[] {"CREATE SCHEMA " + fullNamespace};
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
      boolean hasDifferentClusteringOrders, String schema, String table, TableMetadata metadata) {
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
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    return new String[] {"CREATE SCHEMA " + enclose(metadataSchema)};
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // SQL error code -601: Name already used
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
    return "SELECT 1 FROM " + fullTableName + " LIMIT 1";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName);
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithLimitQuery(builder, limit);
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
    // SPACE indexspace-name CONSTRAINS COLUMNS OF THE TABLE SO NO TWO ROWS CAN CONTAIN DUPLICATE
    // VALUES IN THOSE COLUMNS. RID OF EXISTING ROW IS X record-id
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
  public String getDataTypeForKey(DataType dataType) {
    // Primary key columns must be constrained with NOT NULL
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
  @Nullable
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
  public String getTextType(int charLength, boolean isKey) {
    String ret = "VARCHAR(" + charLength + ")";
    if (isKey) {
      ret += " NOT NULL";
    }
    return ret;
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "true" : "false";
  }

  @Override
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM syscat.schemata WHERE schemaname = ?";
  }

  @Override
  public DateColumn parseDateColumn(ResultSet resultSet, String columnName) throws SQLException {
    String dateStr = resultSet.getString(columnName);
    if (dateStr == null) {
      return DateColumn.ofNull(columnName);
    } else {
      LocalDate date = RdbEngineTimeTypeDb2.DATE_FORMATTER.parse(dateStr, LocalDate::from);
      return DateColumn.of(columnName, date);
    }
  }

  @Override
  public TimeColumn parseTimeColumn(ResultSet resultSet, String columnName) throws SQLException {
    // resultSet.getObject(columnName, LocalTime.class) throws a NullPointerException when the
    // column value is null so use resultSet.getTimestamp(columnName) instead
    Timestamp time = resultSet.getTimestamp(columnName);
    if (time == null) {
      return TimeColumn.ofNull(columnName);
    } else {
      return TimeColumn.of(columnName, time.toLocalDateTime().toLocalTime());
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
    // With this Db2 will return a textual description of the error when
    // calling JDBC `SQLException.getMessage` instead of a short message containing only error codes
    props.put(DB2BaseDataSource.propertyKey_retrieveMessagesFromServerOnGetMessage, "true");

    // With this db2 will not make adjustment for Db2 TIMESTAMP type:
    // - for daylight saving time
    // - for the problematic period of October 5, 1582, through October 14, 1582 because the Julian
    // to Gregorian calendar conversion
    // Cf.
    // https://www.ibm.com/docs/en/db2/12.1.0?topic=dttmddtija-date-time-timestamp-values-that-can-cause-problems-in-jdbc-sqlj-applications
    props.put(DB2BaseDataSource.propertyKey_sqljAvoidTimeStampConversion, "true");

    //  By default, calling `ResultSet.next()` when the cursor is already set after the last row
    //  will throw an exception because the cursor is automatically closed. This differs from other
    //  JDBC storages which return false in this case.
    //  By setting this property, the Db2 adapter will match the other storages behaviors with one
    //  difference being executing `ResultSet.next()` won't throw an exception if the ResultSet is
    //  already closed.
    props.put(
        DB2BaseDataSource.propertyKey_allowNextOnExhaustedResultSet,
        String.valueOf(DB2BaseDataSource.YES));
    return props.build();
  }

  @Override
  public String truncateTableSql(String namespace, String table) {
    return "TRUNCATE TABLE " + encloseFullTableName(namespace, table) + " IMMEDIATE";
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? null : escape;
  }

  @Override
  public String getProjectionsSqlForSelectQuery(
      TableMetadata metadata, List<String> originalProjections) {
    // When selecting a DATE column, special handling is required. See RdbEngineDb2#getProjection().
    if (originalProjections.isEmpty()
        && !metadata.getColumnDataTypes().containsValue(DataType.DATE)) {
      return "*";
    }
    Collection<String> projections =
        originalProjections.isEmpty() ? metadata.getColumnNames() : originalProjections;

    return projections.stream()
        .map(columnName -> getProjection(columnName, metadata.getColumnDataType(columnName)))
        .collect(Collectors.joining(","));
  }

  private String getProjection(String columnName, DataType dataType) {
    if (dataType == DataType.DATE) {
      // Selecting a DATE column requires special handling. We need to cast the DATE column values
      // to CHAR and the value will be needed to be parsed as a string.
      // This is required to read correctly DATE values between the period of October 5,
      // 1582, through October 14, 1582 because of the Julian to Gregorian calendar transition
      return "CHAR(" + enclose(columnName) + ") AS " + enclose(columnName);
    }
    return enclose(columnName);
  }
}
