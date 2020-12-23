package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Immutable
public class JdbcDatabaseConfig extends DatabaseConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDatabaseConfig.class);

  public static final String PREFIX = DatabaseConfig.PREFIX + "jdbc.";
  public static final String CONNECTION_POOL_MIN_IDLE = PREFIX + "connection.pool.min_idle";
  public static final String CONNECTION_POOL_MAX_IDLE = PREFIX + "connection.pool.max_idle";
  public static final String CONNECTION_POOL_MAX_TOTAL = PREFIX + "connection.pool.max_total";
  public static final String PREPARED_STATEMENTS_POOL_ENABLED =
      PREFIX + "prepared_statements.pool.enabled";
  public static final String PREPARED_STATEMENTS_POOL_MAX_OPEN =
      PREFIX + "prepared_statements.pool.max_open";
  public static final int DEFAULT_CONNECTION_POOL_MIN_IDLE = 5;
  public static final int DEFAULT_CONNECTION_POOL_MAX_IDLE = 10;
  public static final int DEFAULT_CONNECTION_POOL_MAX_TOTAL = 25;
  public static final boolean DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED = false;
  public static final int DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN = -1;

  private int connectionPoolMinIdle;
  private int connectionPoolMaxIdle;
  private int connectionPoolMaxTotal;
  private boolean preparedStatementsPoolEnabled;
  private int preparedStatementsPoolMaxOpen;

  public JdbcDatabaseConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public JdbcDatabaseConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public JdbcDatabaseConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    super.load();

    connectionPoolMinIdle = getInt(CONNECTION_POOL_MIN_IDLE, DEFAULT_CONNECTION_POOL_MIN_IDLE);
    connectionPoolMaxIdle = getInt(CONNECTION_POOL_MAX_IDLE, DEFAULT_CONNECTION_POOL_MAX_IDLE);
    connectionPoolMaxTotal = getInt(CONNECTION_POOL_MAX_TOTAL, DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    preparedStatementsPoolEnabled =
        getBoolean(PREPARED_STATEMENTS_POOL_ENABLED, DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    preparedStatementsPoolMaxOpen =
        getInt(PREPARED_STATEMENTS_POOL_MAX_OPEN, DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);
  }

  private int getInt(String name, int defaultValue) {
    String value = props.getProperty(name);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      LOGGER.warn(
          "the specified value of '{}' is not a number. using the default value: {}",
          name,
          defaultValue);
      return defaultValue;
    }
  }

  private boolean getBoolean(String name, boolean defaultValue) {
    String value = props.getProperty(name);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
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
}
