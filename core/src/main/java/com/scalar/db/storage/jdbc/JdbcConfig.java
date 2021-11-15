package com.scalar.db.storage.jdbc;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class JdbcConfig extends DatabaseConfig {
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

    connectionPoolMinIdle =
        getInt(getProperties(), CONNECTION_POOL_MIN_IDLE, DEFAULT_CONNECTION_POOL_MIN_IDLE);
    connectionPoolMaxIdle =
        getInt(getProperties(), CONNECTION_POOL_MAX_IDLE, DEFAULT_CONNECTION_POOL_MAX_IDLE);
    connectionPoolMaxTotal =
        getInt(getProperties(), CONNECTION_POOL_MAX_TOTAL, DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    preparedStatementsPoolEnabled =
        getBoolean(
            getProperties(),
            PREPARED_STATEMENTS_POOL_ENABLED,
            DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    preparedStatementsPoolMaxOpen =
        getInt(
            getProperties(),
            PREPARED_STATEMENTS_POOL_MAX_OPEN,
            DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);

    tableMetadataSchema = getString(getProperties(), TABLE_METADATA_SCHEMA, null);
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
