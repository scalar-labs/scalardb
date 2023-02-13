package com.scalar.db.storage.jdbc;

import java.sql.Connection;
import org.apache.commons.dbcp2.BasicDataSource;

public final class JdbcUtils {
  private JdbcUtils() {}

  public static BasicDataSource initDataSource(JdbcConfig config) {
    return initDataSource(config, false);
  }

  public static BasicDataSource initDataSource(JdbcConfig config, boolean transactional) {
    BasicDataSource dataSource = new BasicDataSource();
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
    return dataSource;
  }

  public static BasicDataSource initDataSourceForTableMetadata(JdbcConfig config) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);
    dataSource.setMinIdle(config.getTableMetadataConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getTableMetadataConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getTableMetadataConnectionPoolMaxTotal());
    return dataSource;
  }

  public static BasicDataSource initDataSourceForAdmin(JdbcConfig config) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.getJdbcUrl());
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);
    dataSource.setMinIdle(config.getAdminConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getAdminConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getAdminConnectionPoolMaxTotal());
    return dataSource;
  }
}
