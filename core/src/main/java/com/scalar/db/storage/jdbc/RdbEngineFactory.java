package com.scalar.db.storage.jdbc;

import com.scalar.db.common.error.CoreError;

/** Factory class of subclasses of {@link RdbEngineStrategy} */
public final class RdbEngineFactory {
  private RdbEngineFactory() {
    throw new AssertionError();
  }

  public static RdbEngineStrategy create(JdbcConfig config) {
    String jdbcUrl = config.getJdbcUrl();

    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return new RdbEngineMysql(config);
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return new RdbEnginePostgresql();
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return new RdbEngineOracle(config);
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return new RdbEngineSqlServer();
    } else if (jdbcUrl.startsWith("jdbc:sqlite:")) {
      return new RdbEngineSqlite();
    } else if (jdbcUrl.startsWith("jdbc:yugabytedb:")) {
      return new RdbEngineYugabyte();
    } else if (jdbcUrl.startsWith("jdbc:mariadb:")) {
      return new RdbEngineMariaDB();
    } else if (jdbcUrl.startsWith("jdbc:db2:")) {
      return new RdbEngineDb2(config);
    } else {
      throw new IllegalArgumentException(
          CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED.buildMessage(jdbcUrl));
    }
  }
}
