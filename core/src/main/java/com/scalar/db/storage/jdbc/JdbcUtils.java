package com.scalar.db.storage.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import javax.annotation.Nullable;
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

  /**
   * @param config a jdbc config
   * @return the data source
   * @deprecated As of release 3.5.0. Will be removed in release 4.0.0.
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public static BasicDataSource initDataSource(JdbcConfig config) {
    return initDataSource(config, false, null);
  }

  public static BasicDataSource initDataSource(JdbcConfig config, @Nullable Isolation isolation) {
    return initDataSource(config, false, isolation);
  }

  public static BasicDataSource initDataSource(
      JdbcConfig config, boolean transactional, @Nullable Isolation isolation) {
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
      // if transactional, the default isolation level is SERIALIZABLE
      dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    if (isolation != null) {
      switch (isolation) {
        case READ_UNCOMMITTED:
          dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
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

  public static boolean isConflictError(SQLException e, RdbEngine rdbEngine) {
    switch (rdbEngine) {
      case MYSQL:
        if (e.getErrorCode() == 1213 || e.getErrorCode() == 1205) {
          // Deadlock found when trying to get lock or Lock wait timeout exceeded
          return true;
        }
        break;
      case POSTGRESQL:
        if (e.getSQLState().equals("40001") || e.getSQLState().equals("40P01")) {
          // Serialization error happened or Dead lock found
          return true;
        }
        break;
      case ORACLE:
        if (e.getErrorCode() == 8177 || e.getErrorCode() == 60) {
          // ORA-08177: can't serialize access for this transaction
          // ORA-00060: deadlock detected while waiting for resource
          return true;
        }
        break;
      default:
        break;
    }
    return false;
  }
}
