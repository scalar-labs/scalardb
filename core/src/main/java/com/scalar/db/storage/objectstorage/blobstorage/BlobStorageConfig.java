package com.scalar.db.storage.objectstorage.blobstorage;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStorageConfig implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "blob-storage";
  public static final String STORAGE_NAME_IN_PREFIX = "blob_storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME_IN_PREFIX + ".";

  public static final String PARALLEL_UPLOAD_BLOCK_SIZE_BYTES =
      PREFIX + "parallel_upload_block_size_bytes";
  public static final String PARALLEL_UPLOAD_MAX_CONCURRENCY =
      PREFIX + "parallel_upload_max_concurrency";
  public static final String PARALLEL_UPLOAD_THRESHOLD_SIZE_BYTES =
      PREFIX + "parallel_upload_threshold_size_bytes";
  public static final String REQUEST_TIMEOUT_SECS = PREFIX + "request_timeout_secs";

  private static final Logger logger = LoggerFactory.getLogger(BlobStorageConfig.class);
  private final String endpoint;
  private final String username;
  private final String password;
  private final String bucket;
  private final String metadataNamespace;

  private final Long parallelUploadBlockSizeBytes;
  private final Integer parallelUploadMaxConcurrency;
  private final Long parallelUploadThresholdSizeBytes;
  private final Integer requestTimeoutSecs;

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
    metadataNamespace = databaseConfig.getSystemNamespaceName();

    if (databaseConfig.getScanFetchSize() != DatabaseConfig.DEFAULT_SCAN_FETCH_SIZE) {
      logger.warn(
          "The configuration property \""
              + DatabaseConfig.SCAN_FETCH_SIZE
              + "\" is not applicable to Blob Storage and will be ignored.");
    }

    parallelUploadBlockSizeBytes =
        getLong(databaseConfig.getProperties(), PARALLEL_UPLOAD_BLOCK_SIZE_BYTES, null);
    parallelUploadMaxConcurrency =
        getInt(databaseConfig.getProperties(), PARALLEL_UPLOAD_MAX_CONCURRENCY, null);
    parallelUploadThresholdSizeBytes =
        getLong(databaseConfig.getProperties(), PARALLEL_UPLOAD_THRESHOLD_SIZE_BYTES, null);
    requestTimeoutSecs = getInt(databaseConfig.getProperties(), REQUEST_TIMEOUT_SECS, null);
  }

  @Override
  public String getStorageName() {
    return STORAGE_NAME;
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

  public String getUsername() {
    return username;
  }

  public Optional<Long> getParallelUploadBlockSizeBytes() {
    return Optional.ofNullable(parallelUploadBlockSizeBytes);
  }

  public Optional<Integer> getParallelUploadMaxConcurrency() {
    return Optional.ofNullable(parallelUploadMaxConcurrency);
  }

  public Optional<Long> getParallelUploadThresholdSizeBytes() {
    return Optional.ofNullable(parallelUploadThresholdSizeBytes);
  }

  public Optional<Integer> getRequestTimeoutSecs() {
    return Optional.ofNullable(requestTimeoutSecs);
  }
}
