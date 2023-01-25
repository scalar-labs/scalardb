package com.scalar.db.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/** Factory class of subclasses of {@link RdbEngineStrategy} */
public final class RdbEngineFactory {
  private RdbEngineFactory() {
    throw new AssertionError();
  }

  public static RdbEngineStrategy create(JdbcConfig config) {
    return create(config.getJdbcUrl());
  }

  public static RdbEngineStrategy create(Connection connection) throws SQLException {
    String jdbcUrl = connection.getMetaData().getURL();
    return create(jdbcUrl);
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
}
