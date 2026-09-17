package com.scalar.db.storage.jdbc;

import com.scalar.db.common.CoreError;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

/** Factory class of subclasses of {@link RdbEngineStrategy} */
public final class RdbEngineFactory {
  private RdbEngineFactory() {
    throw new AssertionError();
  }

  public static RdbEngineStrategy create(JdbcConfig config) {
    String jdbcUrl = config.getJdbcUrl();

    if (JdbcUtils.isAwsWrapperUrl(jdbcUrl)) {
      return createForAwsWrapper(config, jdbcUrl);
    }

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
    } else if (jdbcUrl.startsWith("jdbc:cloudspanner:") || jdbcUrl.startsWith("jdbc:spanner:")) {
      return new RdbEngineSpanner(config);
    } else {
      throw new IllegalArgumentException(
          CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED.buildMessage(jdbcUrl));
    }
  }

  /**
   * Selects the engine for a URL that routes through the AWS Advanced JDBC Wrapper.
   *
   * <p>Only Aurora PostgreSQL and Aurora MySQL are supported. The wrapper itself also accepts
   * MariaDB, but allowing a combination that is never tested would let it fail in subtler ways than
   * an unsupported-engine error at startup. TiDB is rejected for the same reason: it shares MySQL's
   * connection string, so it is only recognized after the metadata probe.
   *
   * @param config the config
   * @param jdbcUrl the original JDBC URL, including the wrapper prefix
   * @return the engine for the underlying database
   */
  private static RdbEngineStrategy createForAwsWrapper(JdbcConfig config, String jdbcUrl) {
    String underlyingUrl = JdbcUtils.removeAwsWrapperPrefix(jdbcUrl);

    if (underlyingUrl.startsWith("jdbc:postgresql:")) {
      return new RdbEnginePostgresql();
    } else if (underlyingUrl.startsWith("jdbc:mysql:")) {
      RdbEngineStrategy engine = createMysqlOrTidbEngine(config);
      if (!(engine instanceof RdbEngineTidb)) {
        return engine;
      }
    }

    // Report the URL the user actually configured, not the one left after stripping the prefix.
    throw new IllegalArgumentException(
        CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED_WITH_AWS_ADVANCED_JDBC_WRAPPER.buildMessage(
            jdbcUrl));
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
    try (HikariDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, mysqlEngine);
        Connection connection = dataSource.getConnection()) {
      String version = connection.getMetaData().getDatabaseProductVersion();
      if (version.contains("TiDB")) {
        return new RdbEngineTidb(config);
      } else {
        return mysqlEngine;
      }
    } catch (SQLException e) {
      // We can't throw a checked exception here because it would break backward compatibility since
      // the calling method is executed in constructor of JdbcAdmin or JdbcCrudService
      throw new RuntimeException(
          CoreError.JDBC_MYSQL_GETTING_CONNECTION_METADATA_FAILED.buildMessage(e.getMessage()), e);
    }
  }
}
