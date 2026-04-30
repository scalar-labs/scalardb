package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
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

    /*
     * We need to set the driver class of an underlying database to the dataSource in order
     * to avoid the "No suitable driver" error when ServiceLoader in java.sql.DriverManager doesn't
     * work (e.g., when we dynamically load a driver class from a fatJar).
     */
    hikariConfig.setDriverClassName(rdbEngine.getDriverClassName());

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
