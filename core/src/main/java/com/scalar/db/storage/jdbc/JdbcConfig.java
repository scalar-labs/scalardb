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

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class JdbcConfig extends DatabaseConfig {
  public static final String PREFIX = DatabaseConfig.PREFIX + "jdbc.";
  public static final String CONNECTION_POOL_MIN_IDLE = PREFIX + "connection_pool.min_idle";
  public static final String CONNECTION_POOL_MAX_IDLE = PREFIX + "connection_pool.max_idle";
  public static final String CONNECTION_POOL_MAX_TOTAL = PREFIX + "connection_pool.max_total";
  public static final String PREPARED_STATEMENTS_POOL_ENABLED =
      PREFIX + "prepared_statements_pool.enabled";
  public static final String PREPARED_STATEMENTS_POOL_MAX_OPEN =
      PREFIX + "prepared_statements_pool.max_open";

  public static final String ISOLATION_LEVEL = PREFIX + "isolation_level";

  public static final String TABLE_METADATA_SCHEMA = PREFIX + "table_metadata.schema";
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

  private int connectionPoolMinIdle;
  private int connectionPoolMaxIdle;
  private int connectionPoolMaxTotal;
  private boolean preparedStatementsPoolEnabled;
  private int preparedStatementsPoolMaxOpen;

  @Nullable private Isolation isolation;

  @Nullable private String tableMetadataSchema;
  private int tableMetadataConnectionPoolMinIdle;
  private int tableMetadataConnectionPoolMaxIdle;
  private int tableMetadataConnectionPoolMaxTotal;

  private int adminConnectionPoolMinIdle;
  private int adminConnectionPoolMaxIdle;
  private int adminConnectionPoolMaxTotal;

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

    String isolationLevel = getString(getProperties(), ISOLATION_LEVEL, null);
    if (isolationLevel != null) {
      isolation = Isolation.valueOf(isolationLevel);
    }

    tableMetadataSchema = getString(getProperties(), TABLE_METADATA_SCHEMA, null);
    tableMetadataConnectionPoolMinIdle =
        getInt(
            getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MIN_IDLE,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE);
    tableMetadataConnectionPoolMaxIdle =
        getInt(
            getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MAX_IDLE,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE);
    tableMetadataConnectionPoolMaxTotal =
        getInt(
            getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL);

    adminConnectionPoolMinIdle =
        getInt(
            getProperties(),
            ADMIN_CONNECTION_POOL_MIN_IDLE,
            DEFAULT_ADMIN_CONNECTION_POOL_MIN_IDLE);
    adminConnectionPoolMaxIdle =
        getInt(
            getProperties(),
            ADMIN_CONNECTION_POOL_MAX_IDLE,
            DEFAULT_ADMIN_CONNECTION_POOL_MAX_IDLE);
    adminConnectionPoolMaxTotal =
        getInt(
            getProperties(),
            ADMIN_CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_ADMIN_CONNECTION_POOL_MAX_TOTAL);
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

  public Optional<String> getTableMetadataSchema() {
    return Optional.ofNullable(tableMetadataSchema);
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
