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
  public static final String PREFIX = DatabaseConfig.PREFIX + "cosmos.";
  /** @deprecated As of 5.0, will be removed. Use {@link #METADATA_DATABASE} instead. */
  @Deprecated
  public static final String TABLE_METADATA_DATABASE = PREFIX + "table_metadata.database";

  public static final String METADATA_DATABASE = PREFIX + "metadata.database";
  private final String endpoint;
  private final String key;
  @Nullable private final String metadataDatabase;

  public CosmosConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getProperties().getProperty(DatabaseConfig.STORAGE);
    if (!"cosmos".equals(storage)) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'cosmos'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(DatabaseConfig.CONTACT_POINTS + " is empty");
    }
    endpoint = databaseConfig.getContactPoints().get(0);
    key = databaseConfig.getPassword().orElse(null);

    if (databaseConfig.getProperties().containsKey(METADATA_DATABASE)
        && databaseConfig.getProperties().containsKey(TABLE_METADATA_DATABASE)) {
      throw new IllegalArgumentException(
          "Use either " + METADATA_DATABASE + " or " + TABLE_METADATA_DATABASE + " but not both");
    }
    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_DATABASE)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_DATABASE
              + "\" is deprecated and will be removed in 5.0.0. Please use \""
              + METADATA_DATABASE
              + "\" instead");
      metadataDatabase = getString(databaseConfig.getProperties(), TABLE_METADATA_DATABASE, null);
    } else {
      metadataDatabase = getString(databaseConfig.getProperties(), METADATA_DATABASE, null);
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getKey() {
    return key;
  }

  public Optional<String> getMetadataDatabase() {
    return Optional.ofNullable(metadataDatabase);
  }
}
