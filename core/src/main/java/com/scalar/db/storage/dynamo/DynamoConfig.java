package com.scalar.db.storage.dynamo;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class DynamoConfig {
  private static final Logger logger = LoggerFactory.getLogger(DynamoConfig.class);

  public static final String PREFIX = DatabaseConfig.PREFIX + "dynamo.";
  public static final String ENDPOINT_OVERRIDE = PREFIX + "endpoint_override";
  /** @deprecated As of 5.0, will be removed. Use {@link #METADATA_NAMESPACE} instead. */
  @Deprecated
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  public static final String METADATA_NAMESPACE = PREFIX + "metadata.namespace";
  public static final String NAMESPACE_PREFIX = PREFIX + "namespace.prefix";

  private final String region;
  private final String accessKeyId;
  private final String secretAccessKey;
  @Nullable private final String endpointOverride;
  @Nullable private final String metadataNamespace;
  @Nullable private final String namespacePrefix;

  public DynamoConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!"dynamo".equals(storage)) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'dynamo'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(DatabaseConfig.CONTACT_POINTS + " is empty");
    }
    region = databaseConfig.getContactPoints().get(0);
    accessKeyId = databaseConfig.getUsername().orElse(null);
    secretAccessKey = databaseConfig.getPassword().orElse(null);
    if (databaseConfig.getProperties().containsKey("scalar.db.dynamo.endpoint-override")) {
      logger.warn(
          "The property \"scalar.db.dynamo.endpoint-override\" is deprecated and will be removed in 5.0.0. "
              + "Please use \""
              + ENDPOINT_OVERRIDE
              + "\" instead");
    }
    endpointOverride =
        getString(
            databaseConfig.getProperties(),
            ENDPOINT_OVERRIDE,
            getString(
                databaseConfig.getProperties(),
                "scalar.db.dynamo.endpoint-override", // for backward compatibility
                null));
    if (databaseConfig.getProperties().containsKey(METADATA_NAMESPACE)
        && databaseConfig.getProperties().containsKey(TABLE_METADATA_NAMESPACE)) {
      throw new IllegalArgumentException(
          "Use either " + METADATA_NAMESPACE + " or " + TABLE_METADATA_NAMESPACE + " but not both");
    }
    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_NAMESPACE)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_NAMESPACE
              + "\" is deprecated and will be removed in 5.0.0. Please use \""
              + METADATA_NAMESPACE
              + "\" instead");

      metadataNamespace = getString(databaseConfig.getProperties(), TABLE_METADATA_NAMESPACE, null);
    } else {
      metadataNamespace = getString(databaseConfig.getProperties(), METADATA_NAMESPACE, null);
    }
    namespacePrefix = getString(databaseConfig.getProperties(), NAMESPACE_PREFIX, null);
  }

  public String getRegion() {
    return region;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public Optional<String> getEndpointOverride() {
    return Optional.ofNullable(endpointOverride);
  }

  public Optional<String> getMetadataNamespace() {
    return Optional.ofNullable(metadataNamespace);
  }

  public Optional<String> getNamespacePrefix() {
    return Optional.ofNullable(namespacePrefix);
  }
}
