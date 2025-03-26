package com.scalar.db.storage.objectstorage;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobConfig implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "blob";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String BUCKET = PREFIX + "bucket";

  public static final String PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES =
      PREFIX + "parallel_upload_block_size_in_bytes";
  public static final String PARALLEL_UPLOAD_MAX_PARALLELISM =
      PREFIX + "parallel_upload_max_parallelism";
  public static final String PARALLEL_UPLOAD_THRESHOLD_IN_BYTES =
      PREFIX + "parallel_upload_threshold_in_bytes";
  public static final String REQUEST_TIMEOUT_IN_SECONDS = PREFIX + "request_timeout_in_seconds";

  /** @deprecated As of 5.0, will be removed. */
  @Deprecated
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  private static final Logger logger = LoggerFactory.getLogger(BlobConfig.class);
  private final String endpoint;
  private final String username;
  private final String password;
  private final String bucket;
  private final String metadataNamespace;

  private final long parallelUploadBlockSizeInBytes;
  private final int parallelUploadMaxParallelism;
  private final long parallelUploadThresholdInBytes;
  private final int requestTimeoutInSeconds;

  public BlobConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }
    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    endpoint = databaseConfig.getContactPoints().get(0);
    username = databaseConfig.getUsername().orElse(null);
    password = databaseConfig.getPassword().orElse(null);
    if (!databaseConfig.getProperties().containsKey(BUCKET)) {
      throw new IllegalArgumentException("Bucket name is not specified.");
    }
    bucket = databaseConfig.getProperties().getProperty(BUCKET);

    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_NAMESPACE)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_NAMESPACE
              + "\" is deprecated and will be removed in 5.0.0.");
      metadataNamespace =
          getString(
              databaseConfig.getProperties(),
              TABLE_METADATA_NAMESPACE,
              DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    } else {
      metadataNamespace = databaseConfig.getSystemNamespaceName();
    }

    parallelUploadBlockSizeInBytes =
        getLong(
            databaseConfig.getProperties(), PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES, 50 * 1024 * 1024);
    parallelUploadMaxParallelism =
        getInt(databaseConfig.getProperties(), PARALLEL_UPLOAD_MAX_PARALLELISM, 4);
    parallelUploadThresholdInBytes =
        getLong(
            databaseConfig.getProperties(), PARALLEL_UPLOAD_THRESHOLD_IN_BYTES, 100 * 1024 * 1024);
    requestTimeoutInSeconds =
        getInt(databaseConfig.getProperties(), REQUEST_TIMEOUT_IN_SECONDS, 15);
  }

  @Override
  public String getStorageName() {
    return STORAGE_NAME;
  }

  @Override
  public String getEndpoint() {
    return endpoint;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public String getBucket() {
    return bucket;
  }

  @Override
  public String getMetadataNamespace() {
    return metadataNamespace;
  }

  public long getParallelUploadBlockSizeInBytes() {
    return parallelUploadBlockSizeInBytes;
  }

  public int getParallelUploadMaxParallelism() {
    return parallelUploadMaxParallelism;
  }

  public long getParallelUploadThresholdInBytes() {
    return parallelUploadThresholdInBytes;
  }

  public int getRequestTimeoutInSeconds() {
    return requestTimeoutInSeconds;
  }
}
