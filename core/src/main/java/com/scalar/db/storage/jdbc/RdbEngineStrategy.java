package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import java.sql.Connection;
import java.sql.SQLException;

abstract class RdbEngineStrategy {

  static RdbEngineStrategy create(JdbcConfig config) {
    switch (config.getRdbEngine()) {
      case MYSQL:
        return new RdbEngineMysql();
      case POSTGRESQL:
        return new RdbEnginePostgresql();
      case ORACLE:
        return new RdbEngineOracle();
      case SQL_SERVER:
        return new RdbEngineSqlServer();
      default:
        assert false;
        return null;
    }
  }

  protected abstract RdbEngine getRdbEngine();

  protected abstract String getDataTypeForEngine(DataType dataType);

  protected abstract void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException;

  protected abstract String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  protected abstract void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException;

  protected String enclose(String name) {
    return QueryUtils.enclose(name, getRdbEngine());
  }
}
