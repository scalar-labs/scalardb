package com.scalar.db.storage.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import oracle.jdbc.OracleDriver;
import org.apache.commons.dbcp2.BasicDataSource;

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

  public static BasicDataSource initDataSource(JdbcConfig config) {
    return initDataSource(config, false);
  }

  public static BasicDataSource initDataSource(JdbcConfig config, boolean transactional) {
    String jdbcUrl = config.getContactPoints().get(0);
    BasicDataSource dataSource = new BasicDataSource();

    /*
     * We need to set the driver class of an underlining database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    dataSource.setDriver(getDriverClass(jdbcUrl));

    dataSource.setUrl(jdbcUrl);

    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);

    if (transactional) {
      dataSource.setDefaultAutoCommit(false);
      dataSource.setAutoCommitOnReturn(false);
      //dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      //dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    dataSource.setMinIdle(config.getConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getConnectionPoolMaxTotal());
    dataSource.setPoolPreparedStatements(config.isPreparedStatementsPoolEnabled());
    dataSource.setMaxOpenPreparedStatements(config.getPreparedStatementsPoolMaxOpen());

    // TODO
    //dataSource.setPoolPreparedStatements(true);
    dataSource.setTestOnBorrow(false);
    dataSource.setRollbackOnReturn(false);

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
