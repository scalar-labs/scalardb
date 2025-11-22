package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.JDBCType;
import java.util.Map.Entry;
import org.apache.commons.dbcp2.BasicDataSource;

public final class JdbcUtils {
  private JdbcUtils() {}

  public static BasicDataSource initDataSource(JdbcConfig config, RdbEngineStrategy rdbEngine) {
    return initDataSource(config, rdbEngine, false);
  }

  @VisibleForTesting
  static BasicDataSource createDataSource() {
    return new BasicDataSource();
  }

  public static BasicDataSource initDataSource(
      JdbcConfig config, RdbEngineStrategy rdbEngine, boolean transactional) {
    BasicDataSource dataSource = createDataSource();

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
    }

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                dataSource.setDefaultTransactionIsolation(toJdbcTransactionIsolation(isolation)));

    dataSource.setDefaultReadOnly(false);

    dataSource.setMinIdle(config.getConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getConnectionPoolMaxTotal());
    dataSource.setPoolPreparedStatements(config.isPreparedStatementsPoolEnabled());
    dataSource.setMaxOpenPreparedStatements(config.getPreparedStatementsPoolMaxOpen());
    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      dataSource.addConnectionProperty(entry.getKey(), entry.getValue());
    }

    return dataSource;
  }

  public static BasicDataSource initDataSourceForTableMetadata(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    BasicDataSource dataSource = createDataSource();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(rdbEngine.getDriver());

    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                dataSource.setDefaultTransactionIsolation(toJdbcTransactionIsolation(isolation)));

    dataSource.setDefaultReadOnly(false);

    dataSource.setMinIdle(config.getTableMetadataConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getTableMetadataConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getTableMetadataConnectionPoolMaxTotal());
    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      dataSource.addConnectionProperty(entry.getKey(), entry.getValue());
    }

    return dataSource;
  }

  public static BasicDataSource initDataSourceForAdmin(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    BasicDataSource dataSource = createDataSource();

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(rdbEngine.getDriver());

    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                dataSource.setDefaultTransactionIsolation(toJdbcTransactionIsolation(isolation)));

    dataSource.setDefaultReadOnly(false);

    dataSource.setMinIdle(config.getAdminConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getAdminConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getAdminConnectionPoolMaxTotal());
    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      dataSource.addConnectionProperty(entry.getKey(), entry.getValue());
    }
    return dataSource;
  }

  private static int toJdbcTransactionIsolation(Isolation isolation) {
    switch (isolation) {
      case READ_UNCOMMITTED:
        return Connection.TRANSACTION_READ_UNCOMMITTED;
      case READ_COMMITTED:
        return Connection.TRANSACTION_READ_COMMITTED;
      case REPEATABLE_READ:
        return Connection.TRANSACTION_REPEATABLE_READ;
      case SERIALIZABLE:
        return Connection.TRANSACTION_SERIALIZABLE;
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
}
