package com.scalar.db.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/** Factory class of subclasses of {@link RdbEngineStrategy} */
public class RdbEngineFactory {
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
    } else {
      throw new IllegalArgumentException("the rdb engine is not supported: " + jdbcUrl);
    }
  }
}
