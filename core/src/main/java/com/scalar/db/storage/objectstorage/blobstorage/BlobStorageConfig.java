package com.scalar.db.storage.objectstorage.blobstorage;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStorageConfig implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "blob-storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  public static final String PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES =
      PREFIX + "parallel_upload_block_size_in_bytes";
  public static final String PARALLEL_UPLOAD_MAX_PARALLELISM =
      PREFIX + "parallel_upload_max_parallelism";
  public static final String PARALLEL_UPLOAD_THRESHOLD_IN_BYTES =
      PREFIX + "parallel_upload_threshold_in_bytes";
  public static final String REQUEST_TIMEOUT_IN_SECONDS = PREFIX + "request_timeout_in_seconds";

  private static final Logger logger = LoggerFactory.getLogger(BlobStorageConfig.class);
  private final String endpoint;
  private final String username;
  private final String password;
  private final String bucket;
  private final String metadataNamespace;

  private final Long parallelUploadBlockSizeInBytes;
  private final Integer parallelUploadMaxParallelism;
  private final Long parallelUploadThresholdInBytes;
  private final Integer requestTimeoutInSeconds;

  public BlobStorageConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }
    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    String fullEndpoint = databaseConfig.getContactPoints().get(0);
    int lastSlashIndex = fullEndpoint.lastIndexOf('/');
    if (lastSlashIndex != -1 && lastSlashIndex < fullEndpoint.length() - 1) {
      endpoint = fullEndpoint.substring(0, lastSlashIndex);
      bucket = fullEndpoint.substring(lastSlashIndex + 1);
    } else {
      throw new IllegalArgumentException(
          "Invalid contact points format. Expected: BLOB_URI/CONTAINER_NAME");
    }
    username = databaseConfig.getUsername().orElse(null);
    password = databaseConfig.getPassword().orElse(null);
    metadataNamespace =
        getString(
            databaseConfig.getProperties(),
            TABLE_METADATA_NAMESPACE,
            DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);

    if (databaseConfig.getScanFetchSize() != DatabaseConfig.DEFAULT_SCAN_FETCH_SIZE) {
      logger.warn(
          "The configuration property \""
              + DatabaseConfig.SCAN_FETCH_SIZE
              + "\" is not applicable to Blob Storage and will be ignored.");
    }

    parallelUploadBlockSizeInBytes =
        getLong(databaseConfig.getProperties(), PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES, null);
    parallelUploadMaxParallelism =
        getInt(databaseConfig.getProperties(), PARALLEL_UPLOAD_MAX_PARALLELISM, null);
    parallelUploadThresholdInBytes =
        getLong(databaseConfig.getProperties(), PARALLEL_UPLOAD_THRESHOLD_IN_BYTES, null);
    requestTimeoutInSeconds =
        getInt(databaseConfig.getProperties(), REQUEST_TIMEOUT_IN_SECONDS, null);
  }

  @Override
  public String getStorageName() {
    return STORAGE_NAME;
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

  public String getEndpoint() {
    return endpoint;
  }

  public Optional<Long> getParallelUploadBlockSizeInBytes() {
    return Optional.ofNullable(parallelUploadBlockSizeInBytes);
  }

  public Optional<Integer> getParallelUploadMaxParallelism() {
    return Optional.ofNullable(parallelUploadMaxParallelism);
  }

  public Optional<Long> getParallelUploadThresholdInBytes() {
    return Optional.ofNullable(parallelUploadThresholdInBytes);
  }

  public Optional<Integer> getRequestTimeoutInSeconds() {
    return Optional.ofNullable(requestTimeoutInSeconds);
  }
}
