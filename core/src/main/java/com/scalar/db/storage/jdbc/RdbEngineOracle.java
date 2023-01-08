package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.scalar.db.storage.jdbc.query.Query;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEngineOracle extends RdbEngineStrategy {

  @Override
  protected void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException {
    execute(connection, "CREATE USER " + fullNamespace + " IDENTIFIED BY \"oracle\"");
    execute(connection, "ALTER USER " + fullNamespace + " quota unlimited on USERS");
  }

  @Override
  protected String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + ")) ROWDEPENDENCIES"; // add ROWDEPENDENCIES to the table to improve the performance
  }

  @Override
  protected void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException {
    // Set INITRANS to 3 and MAXTRANS to 255 for the table to improve the
    // performance
    String alterTableStatement =
        "ALTER TABLE "
            + enclosedFullTableName(schema, table, RdbEngine.ORACLE)
            + " INITRANS 3 MAXTRANS 255";
    execute(connection, alterTableStatement);

    if (hasDescClusteringOrder) {
      // Create a unique index for the clustering orders
      String createUniqueIndexStatement =
          "CREATE UNIQUE INDEX "
              + enclose(getFullTableName(schema, table) + "_clustering_order_idx")
              + " ON "
              + enclosedFullTableName(schema, table, RdbEngine.ORACLE)
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
  protected RdbEngine getRdbEngine() {
    return RdbEngine.ORACLE;
  }

  @Override
  protected String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "NUMBER(19)";
      case BLOB:
        return "RAW(2000)";
      case BOOLEAN:
        return "NUMBER(1)";
      case DOUBLE:
        return "BINARY_DOUBLE";
      case FLOAT:
        return "BINARY_FLOAT";
      case INT:
        return "INT";
      case TEXT:
        return "VARCHAR2(4000)";
      default:
        assert false;
        return null;
    }
  }
}
