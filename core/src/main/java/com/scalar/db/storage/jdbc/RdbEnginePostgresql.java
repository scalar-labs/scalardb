package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEnginePostgresql extends AbstractRdbEngine {
  private static final Logger logger = LoggerFactory.getLogger(RdbEnginePostgresql.class);
  private final RdbEngineTimeTypePostgresql timeTypeEngine;

  public RdbEnginePostgresql() {
    timeTypeEngine = new RdbEngineTimeTypePostgresql();
  }

  @Override
  public String[] createSchemaSqls(String fullSchema) {
    return new String[] {"CREATE SCHEMA " + enclose(fullSchema)};
  }

  @Override
  public String[] createSchemaIfNotExistsSqls(String fullSchema) {
    return new String[] {"CREATE SCHEMA IF NOT EXISTS " + enclose(fullSchema)};
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
              + (ifNotExists ? "IF NOT EXISTS " : "")
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
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    return "ALTER TABLE "
        + encloseFullTableName(namespace, table)
        + " ALTER COLUMN"
        + enclose(columnName)
        + " TYPE "
        + columnType;
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " LIMIT 1";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(schema) + "." + enclose(indexName);
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // 42P07: duplicate_table
    return e.getSQLState().equals("42P07");
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // 23505: unique_violation
    return e.getSQLState().equals("23505");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // 42P01: undefined_table
    return e.getSQLState().equals("42P01");
  }

  @Override
  public boolean isConflict(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // 40001: serialization_failure
    // 40P01: deadlock_detected
    return e.getSQLState().equals("40001") || e.getSQLState().equals("40P01");
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    // Since the "IF NOT EXISTS" syntax is used to create an index, we always return false
    return false;
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
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
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "BYTEA";
      case BOOLEAN:
        return "BOOLEAN";
      case DOUBLE:
        return "DOUBLE PRECISION";
      case FLOAT:
        return "REAL";
      case INT:
        return "INT";
      case TEXT:
        return "TEXT";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case TIMESTAMP:
        return "TIMESTAMP";
      case TIMESTAMPTZ:
        return "TIMESTAMP WITH TIME ZONE";
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    // The number 10485760 is due to the maximum length of the character column.
    // https://www.postgresql.org/docs/15/datatype-character.html
    if (dataType == DataType.TEXT) {
      return "VARCHAR(10485760)";
    }
    return null;
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
      case SMALLINT:
        if (typeName.equalsIgnoreCase("smallserial")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to INT)",
            columnDescription,
            typeName);
        return DataType.INT;
      case INTEGER:
        if (typeName.equalsIgnoreCase("serial")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.INT;
      case BIGINT:
        if (typeName.equalsIgnoreCase("bigserial")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        logger.warn(
            "Data type that may be smaller than that of underlying database is assigned: {} (PostgreSQL {} to ScalarDB BIGINT)",
            columnDescription,
            typeName);
        return DataType.BIGINT;
      case REAL:
        return DataType.FLOAT;
      case DOUBLE:
        if (!typeName.equalsIgnoreCase("float8")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.DOUBLE;
      case CHAR:
      case VARCHAR:
        if (!typeName.equalsIgnoreCase("text")) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} ({} to TEXT)",
              columnDescription,
              typeName);
        }
        return DataType.TEXT;
      case BINARY:
        return DataType.BLOB;
      case DATE:
        return DataType.DATE;
      case TIME:
        if (typeName.equalsIgnoreCase("timetz")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.TIME;
      case TIMESTAMP:
        if (typeName.equalsIgnoreCase("timestamptz")) {
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
        return Types.VARBINARY;
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
    return value ? "true" : "false";
  }

  @Override
  public Driver getDriver() {
    return new org.postgresql.Driver();
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    return createIndexSql.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS");
  }

  @Override
  public RdbEngineTimeTypeStrategy<LocalDate, LocalTime, LocalDateTime, OffsetDateTime>
      getTimeTypeStrategy() {
    return timeTypeEngine;
  }
}
