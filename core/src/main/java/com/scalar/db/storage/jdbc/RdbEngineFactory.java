package com.scalar.db.storage.jdbc;

import com.scalar.db.common.error.CoreError;
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

  static RdbEngineStrategy create(String jdbcUrl) {
    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return new RdbEngineMysql();
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return new RdbEnginePostgresql();
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return new RdbEngineOracle();
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return new RdbEngineSqlServer();
    } else if (jdbcUrl.startsWith("jdbc:sqlite:")) {
      return new RdbEngineSqlite();
    } else {
      throw new IllegalArgumentException(
          CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED.buildMessage(jdbcUrl));
    }
  }
}
