package com.scalar.db.storage.jdbc;

import com.google.common.base.Strings;
import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class JdbcConfig extends DatabaseConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConfig.class);

  public static final String PREFIX = DatabaseConfig.PREFIX + "jdbc.";
  public static final String CONNECTION_POOL_MIN_IDLE = PREFIX + "connection_pool.min_idle";
  public static final String CONNECTION_POOL_MAX_IDLE = PREFIX + "connection_pool.max_idle";
  public static final String CONNECTION_POOL_MAX_TOTAL = PREFIX + "connection_pool.max_total";
  public static final String PREPARED_STATEMENTS_POOL_ENABLED =
      PREFIX + "prepared_statements_pool.enabled";
  public static final String PREPARED_STATEMENTS_POOL_MAX_OPEN =
      PREFIX + "prepared_statements_pool.max_open";
  public static final String TABLE_METADATA_SCHEMA = PREFIX + "table_metadata.schema";

  public static final int DEFAULT_CONNECTION_POOL_MIN_IDLE = 20;
  public static final int DEFAULT_CONNECTION_POOL_MAX_IDLE = 50;
  public static final int DEFAULT_CONNECTION_POOL_MAX_TOTAL = 200;
  public static final boolean DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED = false;
  public static final int DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN = -1;

  private int connectionPoolMinIdle;
  private int connectionPoolMaxIdle;
  private int connectionPoolMaxTotal;
  private boolean preparedStatementsPoolEnabled;
  private int preparedStatementsPoolMaxOpen;
  @Nullable private String tableMetadataSchema;

  public JdbcConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public JdbcConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public JdbcConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String storage = getProperties().getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("jdbc")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'jdbc'");
    }

    super.load();

    connectionPoolMinIdle = getInt(CONNECTION_POOL_MIN_IDLE, DEFAULT_CONNECTION_POOL_MIN_IDLE);
    connectionPoolMaxIdle = getInt(CONNECTION_POOL_MAX_IDLE, DEFAULT_CONNECTION_POOL_MAX_IDLE);
    connectionPoolMaxTotal = getInt(CONNECTION_POOL_MAX_TOTAL, DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    preparedStatementsPoolEnabled =
        getBoolean(PREPARED_STATEMENTS_POOL_ENABLED, DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    preparedStatementsPoolMaxOpen =
        getInt(PREPARED_STATEMENTS_POOL_MAX_OPEN, DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);

    if (!Strings.isNullOrEmpty(getProperties().getProperty(TABLE_METADATA_SCHEMA))) {
      tableMetadataSchema = getProperties().getProperty(TABLE_METADATA_SCHEMA);
    }
  }

  private int getInt(String name, int defaultValue) {
    String value = getProperties().getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
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
    String value = getProperties().getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
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

  public Optional<String> getTableMetadataSchema() {
    return Optional.ofNullable(tableMetadataSchema);
  }
}
