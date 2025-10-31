package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithFetchFirstNRowsOnly;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdbEngineOracle extends AbstractRdbEngine {
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
  public String[] createNamespaceSqls(String fullNamespace) {
    return new String[] {
      "CREATE USER " + fullNamespace + " IDENTIFIED BY \"Oracle1234!@#$\"",
      "ALTER USER " + fullNamespace + " quota unlimited on USERS",
    };
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
      boolean hasDifferentClusteringOrders, String schema, String table, TableMetadata metadata) {
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
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    return new String[] {
      "CREATE USER " + enclose(metadataSchema) + " IDENTIFIED BY \"Oracle1234!@#$\"",
      "ALTER USER " + enclose(metadataSchema) + " quota unlimited on USERS",
    };
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
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM " + enclose("ALL_USERS") + " WHERE " + enclose("USERNAME") + " = ?";
  }

  @Override
  public String renameTableSql(String namespace, String oldTableName, String newTableName) {
    return "ALTER TABLE "
        + encloseFullTableName(namespace, oldTableName)
        + " RENAME TO "
        + enclose(newTableName);
  }

  @Override
  public String[] alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    return new String[] {
      "ALTER TABLE "
          + encloseFullTableName(namespace, table)
          + " MODIFY ( "
          + enclose(columnName)
          + " "
          + columnType
          + " )"
    };
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " FETCH FIRST 1 ROWS ONLY";
  }

  @Override
  public String createIndexSql(
      String schema, String table, String indexName, String indexedColumn) {
    return "CREATE INDEX "
        + enclose(schema)
        + "."
        + enclose(indexName)
        + " ON "
        + encloseFullTableName(schema, table)
        + " ("
        + enclose(indexedColumn)
        + ")";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(schema) + "." + enclose(indexName);
  }

  @Override
  public String[] renameIndexSqls(
      String schema, String table, String column, String oldIndexName, String newIndexName) {
    return new String[] {
      "ALTER INDEX "
          + enclose(schema)
          + "."
          + enclose(oldIndexName)
          + " RENAME TO "
          + enclose(newIndexName)
    };
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public SelectQuery buildSelectWithLimitQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithFetchFirstNRowsOnly(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new MergeQuery(builder, "DUAL");
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
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "NUMBER(16)";
      case BLOB:
        return "BLOB";
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
        throw new UnsupportedOperationException(
            CoreError.JDBC_ORACLE_INDEX_OR_KEY_ON_BLOB_COLUMN_NOT_SUPPORTED.buildMessage());
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
  @Nullable
  public String getDataTypeForSecondaryIndex(DataType dataType) {
    if (dataType == DataType.BLOB) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_ORACLE_INDEX_OR_KEY_ON_BLOB_COLUMN_NOT_SUPPORTED.buildMessage());
    } else {
      return super.getDataTypeForSecondaryIndex(dataType);
    }
  }

  @Override
  public void throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
      ScanAll scanAll, TableMetadata metadata) {
    Optional<Scan.Ordering> orderingOnBlobColumn =
        scanAll.getOrderings().stream()
            .filter(
                ordering -> metadata.getColumnDataType(ordering.getColumnName()) == DataType.BLOB)
            .findFirst();
    if (orderingOnBlobColumn.isPresent()) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_ORACLE_CROSS_PARTITION_SCAN_ORDERING_ON_BLOB_COLUMN_NOT_SUPPORTED
              .buildMessage(orderingOnBlobColumn.get()));
    }
  }

  @Override
  public void throwIfConjunctionsColumnNotSupported(
      Set<Conjunction> conjunctions, TableMetadata metadata) {
    Optional<ConditionalExpression> conditionalExpression =
        conjunctions.stream()
            .flatMap(conjunction -> conjunction.getConditions().stream())
            .filter(
                condition ->
                    metadata.getColumnDataType(condition.getColumn().getName()) == DataType.BLOB)
            .findFirst();
    if (conditionalExpression.isPresent()) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_ORACLE_SELECTION_CONDITION_ON_BLOB_COLUMN_NOT_SUPPORTED.buildMessage(
              conditionalExpression.get()));
    }
  }

  @Override
  public RdbEngineTimeTypeStrategy<LocalDate, LocalDateTime, LocalDateTime, OffsetDateTime>
      getTimeTypeStrategy() {
    return timeTypeEngine;
  }

  @Override
  public void throwIfAlterColumnTypeNotSupported(DataType from, DataType to) {
    if (!(from == DataType.INT && to == DataType.BIGINT)) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_ORACLE_UNSUPPORTED_COLUMN_TYPE_CONVERSION.buildMessage(
              from.toString(), to.toString()));
    }
  }

  @Override
  public void bindBlobColumnToPreparedStatement(
      PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
    // When writing to the BLOB data type with a BLOB size greater than 32766 using a MERGE INTO
    // statement, an internal error ORA-03137 on the server side occurs so we needed to use a
    // workaround. This has been confirmed to be a limitation by AWS support.
    // Below is a detailed explanation of the workaround.
    //
    // Depending on the byte array size, the JDBC driver automatically chooses one the following
    // mode to transfer the BLOB data to the server:
    // - DIRECT: the most efficient mode. It's used when the byte array length is less than 32767.
    // - STREAM: this mode is less efficient. It's used when the byte array length is greater than
    // 32766.
    // - LOB BINDING: this mode is the least efficient. It's used when an input stream without
    // specifying the length is specified.
    //
    // When the driver selects the STREAM mode, the error
    // ORA-03137 occurs. So, we work around the issue by making sure to use the driver in a way so
    // that it should never select the STREAM mode.
    // For more details about the modes, see the following documentation:
    // https://docs.oracle.com/en/database/oracle/oracle-database/23/jjdbc/LOBs-and-BFiles.html#GUID-8FD40D53-8D64-4187-9F6F-FF78242188AD
    if (bytes.length <= 32766) {
      // the DIRECT mode is used to send BLOB data of small size
      preparedStatement.setBytes(index, bytes);
    } else {
      // the LOB BINDING mode is used to send BLOB data of large size
      InputStream inputStream = new ByteArrayInputStream(bytes);
      preparedStatement.setBinaryStream(index, inputStream);
    }
  }

  @Override
  public String getTableNamesInNamespaceSql() {
    return "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ?";
  }
}
