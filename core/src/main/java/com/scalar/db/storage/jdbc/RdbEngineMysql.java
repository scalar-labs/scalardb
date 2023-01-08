package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RdbEngineMysql extends RdbEngineStrategy {

  @Override
  protected void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException {
    execute(connection, "CREATE SCHEMA " + fullNamespace + " character set utf8 COLLATE utf8_bin");
  }

  @Override
  protected String createTableInternalPrimaryKeyClause(
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
  protected void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata) {
    // do nothing
  }

  @Override
  protected RdbEngine getRdbEngine() {
    return RdbEngine.MYSQL;
  }

  protected RdbEngineErrorType interpretSqlException(SQLException e) {
    if (e.getErrorCode() == 1049 || e.getErrorCode() == 1146) {
      return RdbEngineErrorType.UNDEFINED_OBJECT;
    } else {
      return RdbEngineErrorType.UNKNOWN;
    }
  }

  @Override
  protected String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BIGINT:
        return "BIGINT";
      case BLOB:
        return "LONGBLOB";
      case BOOLEAN:
        return "BOOLEAN";
      case DOUBLE:
      case FLOAT:
        return "DOUBLE";
      case INT:
        return "INT";
      case TEXT:
        return "LONGTEXT";
      default:
        assert false;
        return null;
    }
  }
}
