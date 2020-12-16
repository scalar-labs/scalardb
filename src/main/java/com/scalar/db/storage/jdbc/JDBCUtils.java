package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;

public final class JDBCUtils {
  private JDBCUtils() {}

  public static RDBType getRDBType(String jdbcUrl) {
    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return RDBType.MYSQL;
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return RDBType.POSTGRESQL;
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return RDBType.ORACLE;
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return RDBType.SQLSERVER;
    } else {
      throw new IllegalArgumentException("the specified rdb is not supported.");
    }
  }

  public static BasicDataSource initDataSource(DatabaseConfig config) {
    return initDataSource(config, true);
  }

  public static BasicDataSource initDataSource(DatabaseConfig config, boolean transactional) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.getContactPoints().get(0));
    dataSource.setUsername(config.getUsername());
    dataSource.setPassword(config.getPassword());

    if (!transactional) {
      dataSource.setDefaultAutoCommit(false);
      dataSource.setAutoCommitOnReturn(false);
      dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    if (config instanceof JDBCDatabaseConfig) {
      JDBCDatabaseConfig jdbcDatabaseConfig = (JDBCDatabaseConfig) config;
      dataSource.setMinIdle(jdbcDatabaseConfig.getConnectionPoolMinIdle());
      dataSource.setMaxIdle(jdbcDatabaseConfig.getConnectionPoolMaxIdle());
      dataSource.setMaxTotal(jdbcDatabaseConfig.getConnectionPoolMaxTotal());
    } else {
      dataSource.setMinIdle(JDBCDatabaseConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
      dataSource.setMaxIdle(JDBCDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
      dataSource.setMaxTotal(JDBCDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    }
    return dataSource;
  }
}
