package com.scalar.db.storage.jdbc;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class JdbcConfig {
  private static final Logger logger = LoggerFactory.getLogger(JdbcConfig.class);
  public static final String PREFIX = DatabaseConfig.PREFIX + "jdbc.";
  public static final String CONNECTION_POOL_MIN_IDLE = PREFIX + "connection_pool.min_idle";
  public static final String CONNECTION_POOL_MAX_IDLE = PREFIX + "connection_pool.max_idle";
  public static final String CONNECTION_POOL_MAX_TOTAL = PREFIX + "connection_pool.max_total";
  public static final String PREPARED_STATEMENTS_POOL_ENABLED =
      PREFIX + "prepared_statements_pool.enabled";
  public static final String PREPARED_STATEMENTS_POOL_MAX_OPEN =
      PREFIX + "prepared_statements_pool.max_open";

  public static final String ISOLATION_LEVEL = PREFIX + "isolation_level";
  /** @deprecated As of 5.0, will be removed. Use {@link #METADATA_SCHEMA} instead. */
  @Deprecated public static final String TABLE_METADATA_SCHEMA = PREFIX + "table_metadata.schema";

  public static final String METADATA_SCHEMA = PREFIX + "metadata.schema";
  public static final String TABLE_METADATA_CONNECTION_POOL_MIN_IDLE =
      PREFIX + "table_metadata.connection_pool.min_idle";
  public static final String TABLE_METADATA_CONNECTION_POOL_MAX_IDLE =
      PREFIX + "table_metadata.connection_pool.max_idle";
  public static final String TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL =
      PREFIX + "table_metadata.connection_pool.max_total";

  public static final String ADMIN_CONNECTION_POOL_MIN_IDLE =
      PREFIX + "admin.connection_pool.min_idle";
  public static final String ADMIN_CONNECTION_POOL_MAX_IDLE =
      PREFIX + "admin.connection_pool.max_idle";
  public static final String ADMIN_CONNECTION_POOL_MAX_TOTAL =
      PREFIX + "admin.connection_pool.max_total";

  public static final int DEFAULT_CONNECTION_POOL_MIN_IDLE = 20;
  public static final int DEFAULT_CONNECTION_POOL_MAX_IDLE = 50;
  public static final int DEFAULT_CONNECTION_POOL_MAX_TOTAL = 200;
  public static final boolean DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED = false;
  public static final int DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN = -1;

  public static final int DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE = 5;
  public static final int DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE = 10;
  public static final int DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL = 25;

  public static final int DEFAULT_ADMIN_CONNECTION_POOL_MIN_IDLE = 5;
  public static final int DEFAULT_ADMIN_CONNECTION_POOL_MAX_IDLE = 10;
  public static final int DEFAULT_ADMIN_CONNECTION_POOL_MAX_TOTAL = 25;

  private final String jdbcUrl;
  @Nullable private final String username;
  @Nullable private final String password;

  private final int connectionPoolMinIdle;
  private final int connectionPoolMaxIdle;
  private final int connectionPoolMaxTotal;
  private final boolean preparedStatementsPoolEnabled;
  private final int preparedStatementsPoolMaxOpen;

  @Nullable private final Isolation isolation;

  @Nullable private final String metadataSchema;
  private final int tableMetadataConnectionPoolMinIdle;
  private final int tableMetadataConnectionPoolMaxIdle;
  private final int tableMetadataConnectionPoolMaxTotal;

  private final int adminConnectionPoolMinIdle;
  private final int adminConnectionPoolMaxIdle;
  private final int adminConnectionPoolMaxTotal;

  public JdbcConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    String transactionManager = databaseConfig.getTransactionManager();
    if (!"jdbc".equals(storage) && !"jdbc".equals(transactionManager)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE
              + " or "
              + DatabaseConfig.TRANSACTION_MANAGER
              + " should be 'jdbc'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(DatabaseConfig.CONTACT_POINTS + " is empty");
    }
    jdbcUrl = databaseConfig.getContactPoints().get(0);
    username = databaseConfig.getUsername().orElse(null);
    password = databaseConfig.getPassword().orElse(null);

    connectionPoolMinIdle =
        getInt(
            databaseConfig.getProperties(),
            CONNECTION_POOL_MIN_IDLE,
            DEFAULT_CONNECTION_POOL_MIN_IDLE);
    connectionPoolMaxIdle =
        getInt(
            databaseConfig.getProperties(),
            CONNECTION_POOL_MAX_IDLE,
            DEFAULT_CONNECTION_POOL_MAX_IDLE);
    connectionPoolMaxTotal =
        getInt(
            databaseConfig.getProperties(),
            CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    preparedStatementsPoolEnabled =
        getBoolean(
            databaseConfig.getProperties(),
            PREPARED_STATEMENTS_POOL_ENABLED,
            DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    preparedStatementsPoolMaxOpen =
        getInt(
            databaseConfig.getProperties(),
            PREPARED_STATEMENTS_POOL_MAX_OPEN,
            DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);

    String isolationLevel = getString(databaseConfig.getProperties(), ISOLATION_LEVEL, null);
    if (isolationLevel != null) {
      isolation = Isolation.valueOf(isolationLevel.toUpperCase(Locale.ROOT));
    } else {
      isolation = null;
    }

    tableMetadataConnectionPoolMinIdle =
        getInt(
            databaseConfig.getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MIN_IDLE,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE);
    tableMetadataConnectionPoolMaxIdle =
        getInt(
            databaseConfig.getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MAX_IDLE,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE);
    tableMetadataConnectionPoolMaxTotal =
        getInt(
            databaseConfig.getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL);

    adminConnectionPoolMinIdle =
        getInt(
            databaseConfig.getProperties(),
            ADMIN_CONNECTION_POOL_MIN_IDLE,
            DEFAULT_ADMIN_CONNECTION_POOL_MIN_IDLE);
    adminConnectionPoolMaxIdle =
        getInt(
            databaseConfig.getProperties(),
            ADMIN_CONNECTION_POOL_MAX_IDLE,
            DEFAULT_ADMIN_CONNECTION_POOL_MAX_IDLE);
    adminConnectionPoolMaxTotal =
        getInt(
            databaseConfig.getProperties(),
            ADMIN_CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_ADMIN_CONNECTION_POOL_MAX_TOTAL);

    if (databaseConfig.getProperties().containsKey(METADATA_SCHEMA)
        && databaseConfig.getProperties().containsKey(TABLE_METADATA_SCHEMA)) {
      throw new IllegalArgumentException(
          "Use either " + METADATA_SCHEMA + " or " + TABLE_METADATA_SCHEMA + " but not both");
    }
    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_SCHEMA)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_SCHEMA
              + "\" is deprecated and will be removed in 5.0.0. Please use \""
              + METADATA_SCHEMA
              + "\" instead");

      metadataSchema = getString(databaseConfig.getProperties(), TABLE_METADATA_SCHEMA, null);
    } else {
      metadataSchema = getString(databaseConfig.getProperties(), METADATA_SCHEMA, null);
    }
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public Optional<String> getUsername() {
    return Optional.ofNullable(username);
  }

  public Optional<String> getPassword() {
    return Optional.ofNullable(password);
  }

  public int getConnectionPoolMinIdle() {
    return connectionPoolMinIdle;
  }

  public int getConnectionPoolMaxIdle() {
    return connectionPoolMaxIdle;
  }

  public int getConnectionPoolMaxTotal() {
    return connectionPoolMaxTotal;
  }

  public boolean isPreparedStatementsPoolEnabled() {
    return preparedStatementsPoolEnabled;
  }

  public int getPreparedStatementsPoolMaxOpen() {
    return preparedStatementsPoolMaxOpen;
  }

  public Optional<Isolation> getIsolation() {
    return Optional.ofNullable(isolation);
  }

  public Optional<String> getMetadataSchema() {
    return Optional.ofNullable(metadataSchema);
  }

  public int getTableMetadataConnectionPoolMinIdle() {
    return tableMetadataConnectionPoolMinIdle;
  }

  public int getTableMetadataConnectionPoolMaxIdle() {
    return tableMetadataConnectionPoolMaxIdle;
  }

  public int getTableMetadataConnectionPoolMaxTotal() {
    return tableMetadataConnectionPoolMaxTotal;
  }

  public int getAdminConnectionPoolMinIdle() {
    return adminConnectionPoolMinIdle;
  }

  public int getAdminConnectionPoolMaxIdle() {
    return adminConnectionPoolMaxIdle;
  }

  public int getAdminConnectionPoolMaxTotal() {
    return adminConnectionPoolMaxTotal;
  }
}
