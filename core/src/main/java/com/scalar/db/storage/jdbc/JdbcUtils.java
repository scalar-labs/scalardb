package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.scalar.db.storage.jdbc.RdbEngineStrategy.UnderlyingDataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.dbcp2.BasicDataSource;

public final class JdbcUtils {
  private JdbcUtils() {}

  public static AutoCloseableDataSource initDataSource(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    return initDataSource(config, rdbEngine, false);
  }

  public static AutoCloseableDataSource initDataSource(
      JdbcConfig config, RdbEngineStrategy rdbEngine, boolean transactional) {
    UnderlyingDataSourceConfig underlyingDataSourceConfig = rdbEngine.getDataSourceConfig(config);
    if (underlyingDataSourceConfig != null) {
      return createHikariCpDataSource(config, underlyingDataSourceConfig, true, transactional);
    }

    BasicDataSource dataSource = new BasicDataSource();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error in case when ServiceLoader in java.sql.DriverManager
     * doesn't work (e.g., when we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(rdbEngine.getDriver());

    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);

    if (transactional) {
      dataSource.setDefaultAutoCommit(false);
      dataSource.setAutoCommitOnReturn(false);
      // if transactional, the default isolation level is SERIALIZABLE
      dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    config
        .getIsolation()
        .ifPresent(
            isolation -> {
              switch (isolation) {
                case READ_UNCOMMITTED:
                  dataSource.setDefaultTransactionIsolation(
                      Connection.TRANSACTION_READ_UNCOMMITTED);
                  break;
                case READ_COMMITTED:
                  dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                  break;
                case REPEATABLE_READ:
                  dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                  break;
                case SERIALIZABLE:
                  dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                  break;
                default:
                  throw new AssertionError();
              }
            });

    dataSource.setMinIdle(config.getConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getConnectionPoolMaxTotal());
    dataSource.setPoolPreparedStatements(config.isPreparedStatementsPoolEnabled());
    dataSource.setMaxOpenPreparedStatements(config.getPreparedStatementsPoolMaxOpen());
    return new DbcpDataSource(dataSource);
  }

  public static AutoCloseableDataSource initDataSourceForTableMetadata(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    UnderlyingDataSourceConfig underlyingDataSourceConfig = rdbEngine.getDataSourceConfig(config);
    if (underlyingDataSourceConfig != null) {
      return createHikariCpDataSource(config, underlyingDataSourceConfig, false, false);
    }

    BasicDataSource dataSource = new BasicDataSource();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(rdbEngine.getDriver());

    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);
    dataSource.setMinIdle(config.getTableMetadataConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getTableMetadataConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getTableMetadataConnectionPoolMaxTotal());
    return new DbcpDataSource(dataSource);
  }

  public static AutoCloseableDataSource initDataSourceForAdmin(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    UnderlyingDataSourceConfig underlyingDataSourceConfig = rdbEngine.getDataSourceConfig(config);
    if (underlyingDataSourceConfig != null) {
      return createHikariCpDataSource(config, underlyingDataSourceConfig, false, false);
    }

    BasicDataSource dataSource = new BasicDataSource();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(rdbEngine.getDriver());

    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);
    dataSource.setMinIdle(config.getAdminConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getAdminConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getAdminConnectionPoolMaxTotal());
    return new DbcpDataSource(dataSource);
  }

  public static boolean isSqlite(JdbcConfig config) {
    return config.getJdbcUrl().startsWith("jdbc:sqlite:");
  }

  /**
   * Get {@code JDBCType} of the specified {@code sqlType}.
   *
   * @param sqlType a type defined in {@code java.sql.Types}
   * @return a JDBCType
   */
  public static JDBCType getJdbcType(int sqlType) {
    JDBCType type;
    switch (sqlType) {
      case 100: // for Oracle BINARY_FLOAT
        type = JDBCType.REAL;
        break;
      case 101: // for Oracle BINARY_DOUBLE
        type = JDBCType.DOUBLE;
        break;
      default:
        try {
          type = JDBCType.valueOf(sqlType);
        } catch (IllegalArgumentException e) {
          type = JDBCType.OTHER;
        }
    }
    return type;
  }

  @VisibleForTesting
  static HikariConfig createHikariConfig(
      JdbcConfig jdbcConfig,
      UnderlyingDataSourceConfig underlyingDataSourceConfig,
      boolean forTransactionManager,
      boolean transactional) {
    HikariConfig hikariConfig = new HikariConfig();

    URI uri;
    try {
      if (!jdbcConfig.getJdbcUrl().startsWith("jdbc:")) {
        throw new AssertionError("Invalid JDBC URL. Jdbc Url: " + jdbcConfig.getJdbcUrl());
      }
      uri = new URI(jdbcConfig.getJdbcUrl().substring("jdbc:".length()));
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Failed to parse JDBC URL. Url: " + jdbcConfig.getJdbcUrl());
    }

    String database = uri.getPath();
    if (database.startsWith("/")) {
      database = database.substring(1);
    }

    Map<String, String> underlyingDbParams = Collections.emptyMap();
    if (uri.getQuery() != null) {
      underlyingDbParams = Splitter.on('&').withKeyValueSeparator('=').split(uri.getQuery());
    }

    hikariConfig.setDataSourceClassName(underlyingDataSourceConfig.dataSourceClassName);
    hikariConfig.setMaximumPoolSize(jdbcConfig.getConnectionPoolMaxTotal());
    if (uri.getHost() != null) {
      hikariConfig.addDataSourceProperty("serverName", uri.getHost());
    }
    if (uri.getPort() > 0) {
      hikariConfig.addDataSourceProperty("portNumber", String.valueOf(uri.getPort()));
    }
    if (uri.getHost() != null) {
      hikariConfig.addDataSourceProperty("serverName", uri.getHost());
    }
    if (!database.isEmpty()) {
      hikariConfig.addDataSourceProperty("databaseName", database);
    }
    jdbcConfig.getUsername().ifPresent(value -> hikariConfig.addDataSourceProperty("user", value));
    jdbcConfig
        .getPassword()
        .ifPresent(value -> hikariConfig.addDataSourceProperty("password", value));
    for (Entry<String, String> kv : underlyingDataSourceConfig.defaultParams.entrySet()) {
      hikariConfig.addDataSourceProperty(kv.getKey(), kv.getValue());
    }
    for (Entry<String, String> kv : underlyingDbParams.entrySet()) {
      hikariConfig.addDataSourceProperty(kv.getKey(), kv.getValue());
    }

    if (forTransactionManager) {
      if (transactional) {
        hikariConfig.setAutoCommit(false);
        // if transactional, the default isolation level is SERIALIZABLE
        hikariConfig.setTransactionIsolation(IsolationLevel.TRANSACTION_SERIALIZABLE.name());
      }

      jdbcConfig
          .getIsolation()
          .ifPresent(
              isolation -> {
                switch (isolation) {
                  case READ_UNCOMMITTED:
                    hikariConfig.setTransactionIsolation(
                        IsolationLevel.TRANSACTION_READ_UNCOMMITTED.name());
                    break;
                  case READ_COMMITTED:
                    hikariConfig.setTransactionIsolation(
                        IsolationLevel.TRANSACTION_READ_COMMITTED.name());
                    break;
                  case REPEATABLE_READ:
                    hikariConfig.setTransactionIsolation(
                        IsolationLevel.TRANSACTION_REPEATABLE_READ.name());
                    break;
                  case SERIALIZABLE:
                    hikariConfig.setTransactionIsolation(
                        IsolationLevel.TRANSACTION_SERIALIZABLE.name());
                    break;
                  default:
                    throw new AssertionError("Unexpected transaction isolation: " + isolation);
                }
              });
    }

    hikariConfig.setMinimumIdle(jdbcConfig.getConnectionPoolMinIdle());
    // TODO: Revisit this.
    // hikariConfig.setMaxIdle(config.getConnectionPoolMaxIdle());
    hikariConfig.setMaximumPoolSize(jdbcConfig.getConnectionPoolMaxTotal());
    // hikariConfig.setPoolPreparedStatements(config.isPreparedStatementsPoolEnabled());
    // hikariConfig.setMaxOpenPreparedStatements(config.getPreparedStatementsPoolMaxOpen());

    hikariConfig.validate();

    return hikariConfig;
  }

  @VisibleForTesting
  static HikariCpDataSource createHikariCpDataSource(
      JdbcConfig jdbcConfig,
      UnderlyingDataSourceConfig underlyingDataSourceConfig,
      boolean forTransactionManager,
      boolean transactional) {
    HikariConfig hikariConfig =
        createHikariConfig(
            jdbcConfig, underlyingDataSourceConfig, forTransactionManager, transactional);
    return new HikariCpDataSource(new HikariDataSource(hikariConfig));
  }
}
