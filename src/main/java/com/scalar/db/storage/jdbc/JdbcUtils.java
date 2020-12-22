package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;

public final class JdbcUtils {
  private JdbcUtils() {}

  public static RdbEngine getRdbEngine(String jdbcUrl) {
    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return RdbEngine.MY_SQL;
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return RdbEngine.POSTGRE_SQL;
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return RdbEngine.ORACLE;
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return RdbEngine.SQL_SERVER;
    } else {
      throw new IllegalArgumentException("the specified rdb engine is not supported.");
    }
  }

  public static BasicDataSource initDataSource(DatabaseConfig config) {
    return initDataSource(config, false);
  }

  public static BasicDataSource initDataSource(DatabaseConfig config, boolean transactional) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.getContactPoints().get(0));
    dataSource.setUsername(config.getUsername());
    dataSource.setPassword(config.getPassword());

    if (transactional) {
      dataSource.setDefaultAutoCommit(false);
      dataSource.setAutoCommitOnReturn(false);
      dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    if (config instanceof JdbcDatabaseConfig) {
      JdbcDatabaseConfig jdbcDatabaseConfig = (JdbcDatabaseConfig) config;
      dataSource.setMinIdle(jdbcDatabaseConfig.getConnectionPoolMinIdle());
      dataSource.setMaxIdle(jdbcDatabaseConfig.getConnectionPoolMaxIdle());
      dataSource.setMaxTotal(jdbcDatabaseConfig.getConnectionPoolMaxTotal());
      dataSource.setPoolPreparedStatements(jdbcDatabaseConfig.isPreparedStatementsPoolEnabled());
      dataSource.setMaxOpenPreparedStatements(
          jdbcDatabaseConfig.getPreparedStatementsPoolMaxOpen());
    } else {
      dataSource.setMinIdle(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
      dataSource.setMaxIdle(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
      dataSource.setMaxTotal(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    }
    return dataSource;
  }
}
