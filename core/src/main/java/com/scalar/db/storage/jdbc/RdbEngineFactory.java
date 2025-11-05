package com.scalar.db.storage.jdbc;

import com.scalar.db.common.CoreError;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

/** Factory class of subclasses of {@link RdbEngineStrategy} */
public final class RdbEngineFactory {
  private RdbEngineFactory() {
    throw new AssertionError();
  }

  public static RdbEngineStrategy create(JdbcConfig config) {
    String jdbcUrl = config.getJdbcUrl();

    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return createMysqlOrTidbEngine(config);
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

  /**
   * This creates a RdbEngine for MySQL or TiDB. Since TiDB uses the same connection string as
   * MySQL, we can't determine if the storage is TiDB or MySQL by parsing the connection string, so
   * we need to establish a connection and check the metadata to tell them apart.
   *
   * @param config the config
   * @return a {@link RdbEngineMysql} or {@link RdbEngineTidb}.
   */
  private static RdbEngineStrategy createMysqlOrTidbEngine(JdbcConfig config) {
    RdbEngineMysql mysqlEngine = new RdbEngineMysql(config);
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, mysqlEngine);
        Connection connection = dataSource.getConnection()) {
      String version = connection.getMetaData().getDatabaseProductVersion();
      if (version.contains("TiDB")) {
        return new RdbEngineTidb(config);
      } else {
        return mysqlEngine;
      }
    } catch (SQLException e) {
      // We can't throw a checked exception here because it would break backward compatibility since
      // the calling method is executed in constructor of JdbcAdmin or JdbcService
      throw new RuntimeException(
          CoreError.JDBC_MYSQL_GETTING_CONNECTION_METADATA_FAILED.buildMessage(e), e);
    }
  }
}
