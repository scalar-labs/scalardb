package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithLimitQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEnginePostgresql implements RdbEngineStrategy {

  @Override
  public void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException {
    execute(connection, "CREATE SCHEMA " + fullNamespace);
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

  @SuppressFBWarnings({"SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
  @Override
  public void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException {
    if (hasDescClusteringOrder) {
      // Create a unique index for the clustering orders
      String createUniqueIndexStatement =
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
              + ")";
      execute(connection, createUniqueIndexStatement);
    }
  }

  @Override
  public void createMetadataTableIfNotExistsExecute(
      Connection connection, String createTableStatement) throws SQLException {
    String createTableIfNotExistsStatement =
        createTableStatement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
    execute(connection, createTableIfNotExistsStatement);
  }

  @Override
  public void createMetadataSchemaIfNotExists(Connection connection, String metadataSchema)
      throws SQLException {
    execute(connection, "CREATE SCHEMA IF NOT EXISTS " + enclose(metadataSchema));
  }

  @Override
  public void deleteMetadataSchema(Connection connection, String metadataSchema)
      throws SQLException {
    execute(connection, "DROP SCHEMA " + enclose(metadataSchema));
  }

  @Override
  public void dropNamespace(BasicDataSource dataSource, String namespace)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, "DROP SCHEMA " + enclose(namespace));
    } catch (SQLException e) {
      throw new ExecutionException(String.format("error dropping the schema %s", namespace), e);
    }
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
  public void alterColumnType(
      Connection connection, String namespace, String table, String columnName, String columnType)
      throws SQLException {
    String alterColumnStatement =
        "ALTER TABLE "
            + encloseFullTableName(namespace, table)
            + " ALTER COLUMN"
            + enclose(columnName)
            + " TYPE "
            + columnType;
    execute(connection, alterColumnStatement);
  }

  @Override
  public void tableExistsInternalExecuteTableCheck(Connection connection, String fullTableName)
      throws SQLException {
    String tableExistsStatement = "SELECT 1 FROM " + fullTableName + " LIMIT 1";
    execute(connection, tableExistsStatement);
  }

  @Override
  public void dropIndexExecute(Connection connection, String schema, String table, String indexName)
      throws SQLException {
    String dropIndexStatement = "DROP INDEX " + enclose(schema) + "." + enclose(indexName);
    execute(connection, dropIndexStatement);
  }

  @Override
  public boolean isDuplicateUserError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDuplicateSchemaError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // 23505: unique_violation
    return e.getSQLState().equals("23505");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // 42P01: undefined_table
    return e.getSQLState().equals("42P01");
  }

  @Override
  public boolean isConflictError(SQLException e) {
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
    switch (dataType) {
      case TEXT:
        return "VARCHAR(10485760)";
      default:
        return null;
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
}
