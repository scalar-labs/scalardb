package com.scalar.db.storage.objectstorage.s3;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Config implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "s3";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";

  public static final String PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES =
      PREFIX + "parallel_upload_block_size_in_bytes";
  public static final String PARALLEL_UPLOAD_MAX_PARALLELISM =
      PREFIX + "parallel_upload_max_parallelism";
  public static final String PARALLEL_UPLOAD_THRESHOLD_IN_BYTES =
      PREFIX + "parallel_upload_threshold_in_bytes";
  public static final String REQUEST_TIMEOUT_IN_SECONDS = PREFIX + "request_timeout_in_seconds";

  public static final long DEFAULT_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES = 8 * 1024 * 1024; // 8MB
  public static final int DEFAULT_PARALLEL_UPLOAD_MAX_PARALLELISM = 50;
  public static final long DEFAULT_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES = 8 * 1024 * 1024; // 8MB
  public static final int DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 15;

  private static final Logger logger = LoggerFactory.getLogger(S3Config.class);
  private final String username;
  private final String password;
  private final String bucket;
  private final String metadataNamespace;
  private final String region;

  private final long parallelUploadBlockSizeInBytes;
  private final int parallelUploadMaxParallelism;
  private final long parallelUploadThresholdInBytes;
  private final int requestTimeoutInSeconds;

  public S3Config(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }
    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    String contactPoints = databaseConfig.getContactPoints().get(0);
    int lastSlashIndex = contactPoints.lastIndexOf('/');
    if (lastSlashIndex != -1 && lastSlashIndex < contactPoints.length() - 1) {
      region = contactPoints.substring(0, lastSlashIndex);
      bucket = contactPoints.substring(lastSlashIndex + 1);
    } else {
      throw new IllegalArgumentException(
          "Invalid contact points format. Expected: S3_REGION/BUCKET_NAME");
    }
    username = databaseConfig.getUsername().orElse(null);
    password = databaseConfig.getPassword().orElse(null);
    metadataNamespace = databaseConfig.getSystemNamespaceName();

    if (databaseConfig.getScanFetchSize() != DatabaseConfig.DEFAULT_SCAN_FETCH_SIZE) {
      logger.warn(
          "The configuration property \""
              + DatabaseConfig.SCAN_FETCH_SIZE
              + "\" is not applicable to S3 and will be ignored.");
    }

    parallelUploadBlockSizeInBytes =
        getLong(
            databaseConfig.getProperties(),
            PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES,
            DEFAULT_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    parallelUploadMaxParallelism =
        getInt(
            databaseConfig.getProperties(),
            PARALLEL_UPLOAD_MAX_PARALLELISM,
            DEFAULT_PARALLEL_UPLOAD_MAX_PARALLELISM);
    parallelUploadThresholdInBytes =
        getLong(
            databaseConfig.getProperties(),
            PARALLEL_UPLOAD_THRESHOLD_IN_BYTES,
            DEFAULT_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    requestTimeoutInSeconds =
        getInt(
            databaseConfig.getProperties(),
            REQUEST_TIMEOUT_IN_SECONDS,
            DEFAULT_REQUEST_TIMEOUT_IN_SECONDS);
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

  public String getRegion() {
    return region;
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
