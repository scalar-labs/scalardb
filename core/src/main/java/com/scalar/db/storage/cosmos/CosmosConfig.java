package com.scalar.db.storage.cosmos;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class CosmosConfig {
  private static final Logger logger = LoggerFactory.getLogger(CosmosConfig.class);

  public static final String STORAGE_NAME = "cosmos";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";

  /** @deprecated As of 5.0, will be removed. */
  @Deprecated
  public static final String TABLE_METADATA_DATABASE = PREFIX + "table_metadata.database";

  public static final String CONSISTENCY_LEVEL = PREFIX + "consistency_level";

  private final String endpoint;
  private final String key;
  private final String metadataDatabase;
  @Nullable private final String consistencyLevel;

  public CosmosConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(DatabaseConfig.CONTACT_POINTS + " is empty");
    }
    endpoint = databaseConfig.getContactPoints().get(0);
    key = databaseConfig.getPassword().orElse(null);

    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_DATABASE)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_DATABASE
              + "\" is deprecated and will be removed in 5.0.0.");
      metadataDatabase =
          getString(
              databaseConfig.getProperties(),
              TABLE_METADATA_DATABASE,
              DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    } else {
      metadataDatabase = databaseConfig.getSystemNamespaceName();
    }

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

  public String getMetadataDatabase() {
    return metadataDatabase;
  }

  public Optional<String> getConsistencyLevel() {
    return Optional.ofNullable(consistencyLevel);
  }
}
