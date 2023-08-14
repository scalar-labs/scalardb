package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithTop;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEngineSqlServer implements RdbEngineStrategy {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineSqlServer.class);

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
      boolean hasDescClusteringOrder, String schema, String table, TableMetadata metadata) {
    // do nothing
    return new String[0];
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql;
  }

  @Override
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    return new String[] {"CREATE SCHEMA " + enclose(metadataSchema)};
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
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM "
        + encloseFullTableName("sys", "schemas")
        + " WHERE "
        + enclose("name")
        + " = ?";
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
    // 23000: Integrity constraint violation
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // 208: Invalid object name '%.*ls'.
    return e.getErrorCode() == 208;
  }

  @Override
  public boolean isConflictError(SQLException e) {
    // 1205: Transaction (Process ID %d) was deadlocked on %.*ls resources with another process and
    // has been chosen as the deadlock victim. Rerun the transaction.
    return e.getErrorCode() == 1205;
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
        return "VARCHAR(8000) COLLATE Latin1_General_BIN";
      default:
        assert false;
        return null;
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
              String.format(
                  "Data type %s(%d) is unsupported: %s", typeName, columnSize, columnDescription));
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
              String.format("Data type %s is unsupported: %s", typeName, columnDescription));
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
              String.format("Data type %s is unsupported: %s", typeName, columnDescription));
        }
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
        if (typeName.equalsIgnoreCase("timestamp") || typeName.equalsIgnoreCase("hierarchyid")) {
          throw new IllegalArgumentException(
              String.format("Data type %s is unsupported: %s", typeName, columnDescription));
        }
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      case LONGVARBINARY:
        return DataType.BLOB;
      default:
        throw new IllegalArgumentException(
            String.format("Data type %s is unsupported: %s", typeName, columnDescription));
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
}
