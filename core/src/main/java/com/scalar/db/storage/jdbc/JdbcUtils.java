package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Map.Entry;
import javax.sql.DataSource;

public final class JdbcUtils {
  private JdbcUtils() {}

  public static HikariDataSource initDataSource(JdbcConfig config, RdbEngineStrategy rdbEngine) {
    return initDataSource(config, rdbEngine, false);
  }

  @VisibleForTesting
  static HikariDataSource createDataSource(HikariConfig hikariConfig) {
    return new HikariDataSource(hikariConfig);
  }

  public static HikariDataSource initDataSource(
      JdbcConfig config, RdbEngineStrategy rdbEngine, boolean transactional) {
    HikariConfig hikariConfig = new HikariConfig();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error in case when ServiceLoader in java.sql.DriverManager
     * doesn't work (e.g., when we dynamically load a driver class from a fatJar).
     */
    hikariConfig.setDriverClassName(rdbEngine.getDriverClassName());

    hikariConfig.setJdbcUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(hikariConfig::setUsername);
    config.getPassword().ifPresent(hikariConfig::setPassword);

    if (transactional) {
      hikariConfig.setAutoCommit(false);
    }

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                hikariConfig.setTransactionIsolation(toHikariTransactionIsolation(isolation)));

    hikariConfig.setReadOnly(false);

    hikariConfig.setMinimumIdle(config.getConnectionPoolMinIdle());
    hikariConfig.setMaximumPoolSize(config.getConnectionPoolMaxTotal());

    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
    }

    return createDataSource(hikariConfig);
  }

  public static HikariDataSource initDataSourceForTableMetadata(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    HikariConfig hikariConfig = new HikariConfig();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    hikariConfig.setDriverClassName(rdbEngine.getDriverClassName());

    hikariConfig.setJdbcUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(hikariConfig::setUsername);
    config.getPassword().ifPresent(hikariConfig::setPassword);

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                hikariConfig.setTransactionIsolation(toHikariTransactionIsolation(isolation)));

    hikariConfig.setReadOnly(false);

    hikariConfig.setMinimumIdle(config.getTableMetadataConnectionPoolMinIdle());
    hikariConfig.setMaximumPoolSize(config.getTableMetadataConnectionPoolMaxTotal());

    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
    }

    return createDataSource(hikariConfig);
  }

  public static HikariDataSource initDataSourceForAdmin(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    HikariConfig hikariConfig = new HikariConfig();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    hikariConfig.setDriverClassName(rdbEngine.getDriverClassName());

    hikariConfig.setJdbcUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(hikariConfig::setUsername);
    config.getPassword().ifPresent(hikariConfig::setPassword);

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                hikariConfig.setTransactionIsolation(toHikariTransactionIsolation(isolation)));

    hikariConfig.setReadOnly(false);

    hikariConfig.setMinimumIdle(config.getAdminConnectionPoolMinIdle());
    hikariConfig.setMaximumPoolSize(config.getAdminConnectionPoolMaxTotal());

    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
    }

    return createDataSource(hikariConfig);
  }

  private static String toHikariTransactionIsolation(Isolation isolation) {
    switch (isolation) {
      case READ_UNCOMMITTED:
        return "TRANSACTION_READ_UNCOMMITTED";
      case READ_COMMITTED:
        return "TRANSACTION_READ_COMMITTED";
      case REPEATABLE_READ:
        return "TRANSACTION_REPEATABLE_READ";
      case SERIALIZABLE:
        return "TRANSACTION_SERIALIZABLE";
      default:
        throw new AssertionError();
    }
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

  /**
   * Determines whether explicit commit is required for single operations based on the connection's
   * transaction isolation level.
   *
   * @param dataSource the data source to get a connection from
   * @param rdbEngine the RDB engine strategy
   * @return true if explicit commit is required, false otherwise
   */
  public static boolean requiresExplicitCommit(DataSource dataSource, RdbEngineStrategy rdbEngine) {
    try (Connection connection = dataSource.getConnection()) {
      return rdbEngine.requiresExplicitCommit(connection.getTransactionIsolation());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to get transaction isolation level", e);
    }
  }
}
