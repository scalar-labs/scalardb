package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RdbEnginePostgresql extends RdbEngineStrategy {

  @Override
  protected void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException {
    execute(connection, "CREATE SCHEMA " + fullNamespace);
  }

  @Override
  protected String createTableInternalPrimaryKeyClause(
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
  protected void createTableInternalExecuteAfterCreateTable(
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
  boolean isDuplicateUserError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean isDuplicateSchemaError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean isDuplicateTableError(SQLException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean isDuplicateKeyError(SQLException e) {
    // 23505: unique_violation
    return e.getSQLState().equals("23505");
  }

  @Override
  boolean isUndefinedTableError(SQLException e) {
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
  public RdbEngine getRdbEngine() {
    return RdbEngine.POSTGRESQL;
  }

  @Override
  protected String getDataTypeForEngine(DataType scalarDbDataType) {
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
}
