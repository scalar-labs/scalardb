package com.scalar.db.storage.cosmos;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CosmosConfig {
  public static final String STORAGE_NAME = "cosmos";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String TABLE_METADATA_DATABASE = PREFIX + "table_metadata.database";

  public static final String CONSISTENCY_LEVEL = PREFIX + "consistency_level";

  private final String endpoint;
  private final String key;
  @Nullable private final String tableMetadataDatabase;
  @Nullable private final String consistencyLevel;

  public CosmosConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    endpoint = databaseConfig.getContactPoints().get(0);
    key = databaseConfig.getPassword().orElse(null);
    tableMetadataDatabase =
        getString(databaseConfig.getProperties(), TABLE_METADATA_DATABASE, null);
    consistencyLevel = getString(databaseConfig.getProperties(), CONSISTENCY_LEVEL, null);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public String getEndpoint() {
    return endpoint;
  }

  public String getKey() {
    return key;
  }

  public Optional<String> getTableMetadataDatabase() {
    return Optional.ofNullable(tableMetadataDatabase);
  }

  public Optional<String> getConsistencyLevel() {
    return Optional.ofNullable(consistencyLevel);
  }
}
