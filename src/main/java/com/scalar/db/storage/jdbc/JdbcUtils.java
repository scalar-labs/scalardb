package com.scalar.db.storage.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import oracle.jdbc.OracleDriver;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

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
    String jdbcUrl = config.getContactPoints().get(0);
    BasicDataSource dataSource = new BasicDataSource();

    /*
     * We need to specify a driver class corresponding to the JDBC URL to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (ex. we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(getDriverClass(jdbcUrl));

    dataSource.setUrl(jdbcUrl);
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

  private static Driver getDriverClass(String jdbcUrl) {
    switch (getRdbEngine(jdbcUrl)) {
      case MYSQL:
        try {
          return new com.mysql.cj.jdbc.Driver();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      case POSTGRESQL:
        return new org.postgresql.Driver();
      case ORACLE:
        return new OracleDriver();
      case SQL_SERVER:
        return new SQLServerDriver();
      default:
        throw new AssertionError();
    }
  }
}
