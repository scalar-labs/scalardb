package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RdbEnginePostgresql implements RdbEngineStrategy {

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
      boolean hasDescClusteringOrder, String schema, String table, TableMetadata metadata) {
    ArrayList<String> sqls = new ArrayList<>();

    if (hasDescClusteringOrder) {
      // Create a unique index for the clustering orders
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
    throw new ExecutionException("dropping the schema failed: " + namespace, e);
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
  public boolean isConflictError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // 40001: serialization_failure
    // 40P01: deadlock_detected
    return e.getSQLState().equals("40001") || e.getSQLState().equals("40P01");
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
        return "FLOAT";
      case INT:
        return "INT";
      case TEXT:
        return "TEXT";
      default:
        assert false;
        return null;
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    if (dataType == DataType.TEXT) {
      return "VARCHAR(10485760)";
    }
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
        return Types.VARBINARY;
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
}
