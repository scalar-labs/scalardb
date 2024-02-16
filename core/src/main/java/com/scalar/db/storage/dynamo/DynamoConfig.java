package com.scalar.db.storage.dynamo;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class DynamoConfig {
  private static final Logger logger = LoggerFactory.getLogger(DynamoConfig.class);

  public static final String STORAGE_NAME = "dynamo";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String ENDPOINT_OVERRIDE = PREFIX + "endpoint_override";
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";
  public static final String NAMESPACE_PREFIX = PREFIX + "namespace.prefix";

  private final String region;
  private final String accessKeyId;
  private final String secretAccessKey;
  @Nullable private final String endpointOverride;
  @Nullable private final String tableMetadataNamespace;
  @Nullable private final String namespacePrefix;

  public DynamoConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
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
    tableMetadataNamespace =
        getString(databaseConfig.getProperties(), TABLE_METADATA_NAMESPACE, null);
    namespacePrefix = getString(databaseConfig.getProperties(), NAMESPACE_PREFIX, null);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

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

  public Optional<String> getTableMetadataNamespace() {
    return Optional.ofNullable(tableMetadataNamespace);
  }

  public Optional<String> getNamespacePrefix() {
    return Optional.ofNullable(namespacePrefix);
  }
}
