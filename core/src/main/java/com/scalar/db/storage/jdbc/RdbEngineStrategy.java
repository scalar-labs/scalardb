package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class RdbEngineStrategy {

  /** Abstracted errors from each RDB engine. */
  public enum RdbEngineErrorType {
    /** Schema, table, column, ... already exists. */
    DUPLICATE_OBJECT,
    /** Schema, table, column, ... are undefined. */
    UNDEFINED_OBJECT,
    /** Serialization error or deadlock found. */
    CONFLICT,

    /** Mainly used when an error from each RDB engine cannot be interpreted. */
    UNKNOWN,
  }

  public abstract RdbEngineErrorType interpretSqlException(SQLException e);

  public static RdbEngineStrategy create(JdbcConfig config) {
    return create(config.getJdbcUrl());
  }

  public static RdbEngineStrategy create(RdbEngine rdbEngine) {
    switch (rdbEngine) {
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

  static RdbEngineStrategy create(String jdbcUrl) {
    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return new RdbEngineMysql();
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return new RdbEnginePostgresql();
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return new RdbEngineOracle();
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return new RdbEngineSqlServer();
    } else {
      throw new IllegalArgumentException("the rdb engine is not supported: " + jdbcUrl);
    }
  }

  public abstract RdbEngine getRdbEngine();

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
