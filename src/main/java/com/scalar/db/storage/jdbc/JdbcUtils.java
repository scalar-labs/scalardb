package com.scalar.db.storage.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;

public final class JdbcUtils {
  private JdbcUtils() {}

  public static RdbEngine getRdbEngine(String jdbcUrl) {
    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return RdbEngine.MYSQL;
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return RdbEngine.POSTGRESQL;
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return RdbEngine.ORACLE;
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return RdbEngine.SQL_SERVER;
    } else {
      throw new IllegalArgumentException("the rdb engine is not supported: " + jdbcUrl);
    }
  }

  public static BasicDataSource initDataSource(JdbcDatabaseConfig config) {
    return initDataSource(config, false);
  }

  public static BasicDataSource initDataSource(JdbcDatabaseConfig config, boolean transactional) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.getContactPoints().get(0));
    dataSource.setUsername(config.getUsername());
    dataSource.setPassword(config.getPassword());

    if (transactional) {
      dataSource.setDefaultAutoCommit(false);
      dataSource.setAutoCommitOnReturn(false);
      dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    dataSource.setMinIdle(config.getConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getConnectionPoolMaxTotal());
    dataSource.setPoolPreparedStatements(config.isPreparedStatementsPoolEnabled());
    dataSource.setMaxOpenPreparedStatements(config.getPreparedStatementsPoolMaxOpen());

    return dataSource;
  }
}
