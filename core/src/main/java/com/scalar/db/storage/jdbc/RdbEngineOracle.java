package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeIntoQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithFetchFirstNRowsOnly;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEngineOracle extends AbstractRdbEngine {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineOracle.class);
  private final String keyColumnSize;
  private final RdbEngineTimeTypeOracle timeTypeEngine;

  RdbEngineOracle(JdbcConfig config) {
    keyColumnSize = String.valueOf(config.getOracleVariableKeyColumnSize());
    this.timeTypeEngine = new RdbEngineTimeTypeOracle(config);
  }

  @VisibleForTesting
  RdbEngineOracle() {
    keyColumnSize = String.valueOf(JdbcConfig.DEFAULT_VARIABLE_KEY_COLUMN_SIZE);
    timeTypeEngine = null;
  }

  @Override
  public String[] createSchemaSqls(String fullSchema) {
    return new String[] {
      "CREATE USER " + enclose(fullSchema) + " IDENTIFIED BY \"Oracle1234!@#$\"",
      "ALTER USER " + enclose(fullSchema) + " quota unlimited on USERS",
    };
  }

  @Override
  public String[] createSchemaIfNotExistsSqls(String schema) {
    return createSchemaSqls(schema);
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + ")) ROWDEPENDENCIES"; // add ROWDEPENDENCIES to the table to improve the performance
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    ArrayList<String> sqls = new ArrayList<>();

    // Set INITRANS to 3 and MAXTRANS to 255 for the table to improve the
    // performance
    sqls.add("ALTER TABLE " + encloseFullTableName(schema, table) + " INITRANS 3 MAXTRANS 255");

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
    return createTableSql;
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // ORA-01920: user name 'string' conflicts with another user or role name
    return e.getErrorCode() == 1920;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    return "DROP USER " + enclose(metadataSchema);
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    return "DROP USER " + enclose(namespace);
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException {
    throw new ExecutionException("Dropping the user failed: " + namespace, e);
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    return "ALTER TABLE "
        + encloseFullTableName(namespace, table)
        + " MODIFY ( "
        + enclose(columnName)
        + " "
        + columnType
        + " )";
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " FETCH FIRST 1 ROWS ONLY";
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
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithFetchFirstNRowsOnly(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return MergeIntoQuery.createForOracle(builder);
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // ORA-00955: name is already used by an existing object
    return e.getErrorCode() == 955;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // Integrity constraint violation
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // ORA-00942: Table or view does not exist
    return e.getErrorCode() == 942;
  }

  @Override
  public boolean isConflict(SQLException e) {
    // ORA-08177: can't serialize access for this transaction
    // ORA-00060: deadlock detected while waiting for resource
    return e.getErrorCode() == 8177 || e.getErrorCode() == 60;
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    // https://docs.oracle.com/en/error-help/db/ora-00955/
    // code : 955
    // message : name is already used by an existing object
    return e.getErrorCode() == 955;
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "NUMBER(16)";
      case BLOB:
        return "RAW(2000)";
      case BOOLEAN:
        return "NUMBER(1)";
      case DOUBLE:
        return "BINARY_DOUBLE";
      case FLOAT:
        return "BINARY_FLOAT";
      case INT:
        return "NUMBER(10)";
      case TEXT:
        return "VARCHAR2(4000)";
      case DATE:
        return "DATE";
      case TIME:
        return "TIMESTAMP(6)";
      case TIMESTAMP:
        return "TIMESTAMP(3)";
      case TIMESTAMPTZ:
        return "TIMESTAMP(3) WITH TIME ZONE";
      default:
        throw new AssertionError();
    }
  }

  @Override
  @Nullable
  public String getDataTypeForKey(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return "VARCHAR2(" + keyColumnSize + ")";
      case BLOB:
        return "RAW(" + keyColumnSize + ")";
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
    String numericTypeDescription = String.format("%s(%d, %d)", typeName, columnSize, digits);
    switch (type) {
      case NUMERIC:
        if (columnSize > 15) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  numericTypeDescription, columnDescription));
        }
        if (digits == 0) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} to BIGINT",
              numericTypeDescription);
          return DataType.BIGINT;
        } else {
          logger.info(
              "Fixed-point data type is casted, be aware round-up or round-off can be happen in underlying database: {} ({} to DOUBLE)",
              columnDescription,
              numericTypeDescription);
          return DataType.DOUBLE;
        }
      case REAL:
        return DataType.FLOAT;
      case FLOAT:
        if (columnSize > 53) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  numericTypeDescription, columnDescription));
        }
        if (columnSize < 53) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} to DOUBLE",
              numericTypeDescription);
        }
        return DataType.DOUBLE;
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case NCHAR:
      case VARCHAR:
      case NVARCHAR:
      case CLOB:
      case NCLOB:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to TEXT)",
            columnDescription,
            typeName);
        return DataType.TEXT;
      case LONGVARCHAR:
        return DataType.TEXT;
      case VARBINARY:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      case LONGVARBINARY:
        return DataType.BLOB;
      case BLOB:
        logger.warn(
            "Data type that may be smaller than that of underlying database is assigned: {} (Oracle {} to ScalarDB BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      case TIMESTAMP:
        // handles "date" type
        if (typeName.equalsIgnoreCase("date")) {
          if (overrideDataType == DataType.TIME) {
            return DataType.TIME;
          }
          if (overrideDataType == DataType.TIMESTAMP) {
            return DataType.TIMESTAMP;
          }
          return DataType.DATE;
        }
        // handles "timestamp" type
        if (overrideDataType == DataType.TIME) {
          return DataType.TIME;
        }
        return DataType.TIMESTAMP;
      case OTHER:
        if (typeName.toLowerCase().endsWith("time zone")) {
          return DataType.TIMESTAMPTZ;
        }
        throw new IllegalArgumentException(
            CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                typeName, columnDescription));
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
        return Types.BIT;
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
  public String getTextType(int charLength, boolean isKey) {
    return String.format("VARCHAR2(%s)", charLength);
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "1" : "0";
  }

  @Override
  public Driver getDriver() {
    return new oracle.jdbc.driver.OracleDriver();
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? null : escape;
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    return createIndexSql;
  }

  @Override
  public RdbEngineTimeTypeStrategy<LocalDate, LocalDateTime, LocalDateTime, OffsetDateTime>
      getTimeTypeStrategy() {
    return timeTypeEngine;
  }
}
