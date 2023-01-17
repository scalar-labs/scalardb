package com.scalar.db.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
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
      case SQL_SERVER:
        if (e.getErrorCode() == 1205) {
          // Transaction was deadlocked on lock resources with another process and has been chosen
          // as the deadlock victim
          return true;
        }
        break;
      default:
        break;
    }
    return false;
  }
}
