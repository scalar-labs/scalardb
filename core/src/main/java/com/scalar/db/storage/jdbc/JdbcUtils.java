package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.JDBCType;
import java.util.Map.Entry;
import org.apache.commons.dbcp2.BasicDataSource;

public final class JdbcUtils {

  // The maximum index name length is set to 63 to match the shortest limit among supported
  // databases (PostgreSQL). Other databases have higher limits (e.g., MySQL: 64, Oracle: 128,
  // SQL Server: 128).
  @VisibleForTesting static final int MAX_INDEX_NAME_LENGTH = 63;

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

    dataSource.setUrl(rdbEngine.adjustJdbcUrl(config.getJdbcUrl()));
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
    for (Entry<String, String> entry : rdbEngine.getConnectionProperties().entrySet()) {
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

    dataSource.setUrl(rdbEngine.adjustJdbcUrl(config.getJdbcUrl()));
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);
    dataSource.setMinIdle(config.getTableMetadataConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getTableMetadataConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getTableMetadataConnectionPoolMaxTotal());
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

    dataSource.setUrl(rdbEngine.adjustJdbcUrl(config.getJdbcUrl()));
    config.getUsername().ifPresent(dataSource::setUsername);
    config.getPassword().ifPresent(dataSource::setPassword);
    dataSource.setMinIdle(config.getAdminConnectionPoolMinIdle());
    dataSource.setMaxIdle(config.getAdminConnectionPoolMaxIdle());
    dataSource.setMaxTotal(config.getAdminConnectionPoolMaxTotal());
    return dataSource;
  }

  public static boolean isSqlite(JdbcConfig config) {
    return config.getJdbcUrl().startsWith("jdbc:sqlite:");
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
   * Shortens the given index name using a SHA-256 hash if it exceeds the maximum index name length.
   * Returns the original name if it is within the limit. The shortened name is composed of the
   * given prefix followed by a 32-character hex hash.
   *
   * @param name the full index name to check and potentially shorten
   * @param prefix the prefix to preserve in the shortened name (e.g., "index_")
   * @return the original name if within the limit, or a shortened name using a hash
   */
  public static String shortenIndexNameIfNeeded(String name, String prefix) {
    if (name.length() <= MAX_INDEX_NAME_LENGTH) {
      return name;
    }
    // Shorten using SHA-256 hash truncated to 32 hex characters (128 bits)
    String hash =
        Hashing.sha256().hashString(name, StandardCharsets.UTF_8).toString().substring(0, 32);
    return prefix + hash;
  }
}
