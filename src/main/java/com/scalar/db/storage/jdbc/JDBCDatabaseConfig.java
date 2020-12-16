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
public class JDBCDatabaseConfig extends DatabaseConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCDatabaseConfig.class);

  public static final String PREFIX = DatabaseConfig.PREFIX + "jdbc.";
  public static final String CONNECTION_POOL_MIN_IDLE = PREFIX + "connection.pool.min_idle";
  public static final String CONNECTION_POOL_MAX_IDLE = PREFIX + "connection.pool.max_idle";
  public static final String CONNECTION_POOL_MAX_TOTAL = PREFIX + "connection.pool.max_total";
  public static final int DEFAULT_CONNECTION_POOL_MIN_IDLE = 5;
  public static final int DEFAULT_CONNECTION_POOL_MAX_IDLE = 10;
  public static final int DEFAULT_CONNECTION_POOL_MAX_TOTAL = 25;

  private int connectionPoolMinIdle;
  private int connectionPoolMaxIdle;
  private int connectionPoolMaxTotal;

  public JDBCDatabaseConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public JDBCDatabaseConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public JDBCDatabaseConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    super.load();

    connectionPoolMinIdle = getInt(CONNECTION_POOL_MIN_IDLE, DEFAULT_CONNECTION_POOL_MIN_IDLE);
    connectionPoolMaxIdle = getInt(CONNECTION_POOL_MAX_IDLE, DEFAULT_CONNECTION_POOL_MAX_IDLE);
    connectionPoolMaxTotal = getInt(CONNECTION_POOL_MAX_TOTAL, DEFAULT_CONNECTION_POOL_MAX_TOTAL);
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
          "The specified value of '{}' is not a number. Using the default value: {}",
          name,
          defaultValue);
      return defaultValue;
    }
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
}
