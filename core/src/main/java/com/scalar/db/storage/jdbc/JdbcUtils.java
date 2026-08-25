package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.scalar.db.common.CoreError;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import javax.sql.DataSource;

public final class JdbcUtils {

  // The maximum index name length is set to 63 to match the shortest limit among supported
  // databases (PostgreSQL). Other databases have higher limits (e.g., MySQL: 64, Oracle: 128,
  // SQL Server: 128).
  @VisibleForTesting static final int MAX_INDEX_NAME_LENGTH = 63;

  private static final String AWS_WRAPPER_URL_PREFIX = "jdbc:aws-wrapper:";

  private JdbcUtils() {}

  public static HikariDataSource initDataSource(JdbcConfig config, RdbEngineStrategy rdbEngine) {
    return initDataSource(config, rdbEngine, false);
  }

  public static HikariDataSource initDataSource(
      JdbcConfig config, RdbEngineStrategy rdbEngine, boolean transactional) {
    return createDataSource(
        config,
        rdbEngine,
        transactional,
        config.getConnectionPoolMinIdle(),
        config.getConnectionPoolMaxTotal(),
        config.getConnectionPoolConnectionTimeoutMillis().orElse(null),
        config.getConnectionPoolIdleTimeoutMillis().orElse(null),
        config.getConnectionPoolMaxLifetimeMillis().orElse(null),
        config.getConnectionPoolKeepaliveTimeMillis().orElse(null));
  }

  public static HikariDataSource initDataSourceForTableMetadata(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    return createDataSource(
        config,
        rdbEngine,
        false,
        config.getTableMetadataConnectionPoolMinIdle(),
        config.getTableMetadataConnectionPoolMaxTotal(),
        config.getTableMetadataConnectionPoolConnectionTimeoutMillis().orElse(null),
        config.getTableMetadataConnectionPoolIdleTimeoutMillis().orElse(null),
        config.getTableMetadataConnectionPoolMaxLifetimeMillis().orElse(null),
        config.getTableMetadataConnectionPoolKeepaliveTimeMillis().orElse(null));
  }

  public static HikariDataSource initDataSourceForAdmin(
      JdbcConfig config, RdbEngineStrategy rdbEngine) {
    return createDataSource(
        config,
        rdbEngine,
        false,
        config.getAdminConnectionPoolMinIdle(),
        config.getAdminConnectionPoolMaxTotal(),
        config.getAdminConnectionPoolConnectionTimeoutMillis().orElse(null),
        config.getAdminConnectionPoolIdleTimeoutMillis().orElse(null),
        config.getAdminConnectionPoolMaxLifetimeMillis().orElse(null),
        config.getAdminConnectionPoolKeepaliveTimeMillis().orElse(null));
  }

  private static HikariDataSource createDataSource(
      JdbcConfig config,
      RdbEngineStrategy rdbEngine,
      boolean transactional,
      int minIdle,
      int maxTotal,
      @Nullable Long connectionTimeout,
      @Nullable Long idleTimeout,
      @Nullable Long maxLifetime,
      @Nullable Long keepaliveTime) {
    HikariConfig hikariConfig = new HikariConfig();

    // The constructor above also reads whatever the hikaricp.configurationFile system property
    // points at, so this setting can arrive without ScalarDB configuration being involved at all --
    // and AWS documentation recommends setting it when using their JDBC wrapper. Refuse to start
    // rather than run with the inference in JdbcTransaction#commit() quietly broken. That failure
    // would only surface during a failover, would look like an ordinary retry, and would be found
    // by noticing duplicated data.
    if (hikariConfig.getExceptionOverrideClassName() != null) {
      throw new IllegalArgumentException(
          CoreError.JDBC_HIKARICP_EXCEPTION_OVERRIDE_NOT_SUPPORTED.buildMessage(
              hikariConfig.getExceptionOverrideClassName()));
    }

    // Do not set exceptionOverrideClassName here. JdbcTransaction#commit() infers an unknown
    // outcome from HikariCP evicting a connection whose SQLState starts with "08"; see that method
    // for why AWS recommends the override and why setting it would break the inference.

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    hikariConfig.setDriverClassName(getDriverClassName(config, rdbEngine));

    hikariConfig.setJdbcUrl(rdbEngine.adjustJdbcUrl(config.getJdbcUrl()));
    rdbEngine.setConnectionCredentials(config, hikariConfig);

    if (transactional) {
      hikariConfig.setAutoCommit(false);
    }

    config
        .getIsolation()
        .ifPresent(
            isolation ->
                hikariConfig.setTransactionIsolation(toHikariTransactionIsolation(isolation)));

    hikariConfig.setReadOnly(false);
    hikariConfig.setMinimumIdle(minIdle);
    hikariConfig.setMaximumPoolSize(maxTotal);

    if (connectionTimeout != null) {
      hikariConfig.setConnectionTimeout(connectionTimeout);
    }
    if (idleTimeout != null) {
      hikariConfig.setIdleTimeout(idleTimeout);
    }
    if (maxLifetime != null) {
      hikariConfig.setMaxLifetime(maxLifetime);
    }
    if (keepaliveTime != null) {
      hikariConfig.setKeepaliveTime(keepaliveTime);
    }

    for (Entry<String, String> entry : rdbEngine.getConnectionProperties(config).entrySet()) {
      hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
    }

    return createDataSource(hikariConfig);
  }

  @VisibleForTesting
  static HikariDataSource createDataSource(HikariConfig hikariConfig) {
    return new HikariDataSource(hikariConfig);
  }

  static boolean isAwsWrapperUrl(String jdbcUrl) {
    return jdbcUrl != null && jdbcUrl.startsWith(AWS_WRAPPER_URL_PREFIX);
  }

  /**
   * Exposes the URL of the underlying database. Use this only to decide which RDB engine to use:
   * the URL passed to the connection pool must keep the prefix, because that is what makes the
   * wrapper handle the connection.
   */
  static String removeAwsWrapperPrefix(String jdbcUrl) {
    assert isAwsWrapperUrl(jdbcUrl);
    return "jdbc:" + jdbcUrl.substring(AWS_WRAPPER_URL_PREFIX.length());
  }

  /**
   * The AWS Advanced JDBC Wrapper supplies its own driver, so the underlying database's driver
   * class must not be used when the URL routes through it. The engine's own {@code
   * getDriverClassName()} is left untouched; the substitution happens only here.
   */
  private static String getDriverClassName(JdbcConfig config, RdbEngineStrategy rdbEngine) {
    if (isAwsWrapperUrl(config.getJdbcUrl())) {
      return software.amazon.jdbc.Driver.class.getName();
    }
    return rdbEngine.getDriverClassName();
  }

  private static String toHikariTransactionIsolation(Isolation isolation) {
    switch (isolation) {
      case READ_UNCOMMITTED:
        return "TRANSACTION_READ_UNCOMMITTED";
      case READ_COMMITTED:
        return "TRANSACTION_READ_COMMITTED";
      case REPEATABLE_READ:
        return "TRANSACTION_REPEATABLE_READ";
      case SERIALIZABLE:
        return "TRANSACTION_SERIALIZABLE";
      default:
        throw new AssertionError();
    }
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

  /**
   * Determines whether explicit commit is required for single operations based on the connection's
   * transaction isolation level.
   *
   * @param dataSource the data source to get a connection from
   * @param rdbEngine the RDB engine strategy
   * @return true if explicit commit is required, false otherwise
   */
  public static boolean requiresExplicitCommit(DataSource dataSource, RdbEngineStrategy rdbEngine) {
    try (Connection connection = dataSource.getConnection()) {
      return rdbEngine.requiresExplicitCommit(connection.getTransactionIsolation());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to get transaction isolation level", e);
    }
  }
}
