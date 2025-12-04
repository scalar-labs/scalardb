package com.scalar.db.storage.objectstorage.s3;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;

import com.google.common.base.Splitter;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Config implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "s3";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  public static final String MULTIPART_UPLOAD_PART_SIZE_BYTES =
      PREFIX + "multipart_upload_part_size_bytes";
  public static final String MULTIPART_UPLOAD_MAX_CONCURRENCY =
      PREFIX + "multipart_upload_max_concurrency";
  public static final String MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES =
      PREFIX + "multipart_upload_threshold_size_bytes";
  public static final String REQUEST_TIMEOUT_SECS = PREFIX + "request_timeout_secs";

  private static final Logger logger = LoggerFactory.getLogger(S3Config.class);
  private final String username;
  private final String password;
  private final String bucket;
  private final String metadataNamespace;
  private final String region;

  private final Long multipartUploadPartSizeBytes;
  private final Integer multipartUploadMaxConcurrency;
  private final Long multipartUploadThresholdSizeBytes;
  private final Integer requestTimeoutSecs;

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
    List<String> contactPointsParts = Splitter.on('/').splitToList(contactPoints);
    if (contactPointsParts.size() == 2
        && !contactPointsParts.get(0).isEmpty()
        && !contactPointsParts.get(1).isEmpty()) {
      region = contactPointsParts.get(0);
      bucket = contactPointsParts.get(1);
    } else {
      throw new IllegalArgumentException(
          "Invalid contact points format. Expected: S3_REGION/BUCKET_NAME");
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
              + "\" is not applicable to S3 and will be ignored.");
    }

    multipartUploadPartSizeBytes =
        getLong(databaseConfig.getProperties(), MULTIPART_UPLOAD_PART_SIZE_BYTES, null);
    multipartUploadMaxConcurrency =
        getInt(databaseConfig.getProperties(), MULTIPART_UPLOAD_MAX_CONCURRENCY, null);
    multipartUploadThresholdSizeBytes =
        getLong(databaseConfig.getProperties(), MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES, null);
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

  public String getRegion() {
    return region;
  }

  public String getUsername() {
    return username;
  }

  public Optional<Long> getMultipartUploadPartSizeBytes() {
    return Optional.ofNullable(multipartUploadPartSizeBytes);
  }

  public Optional<Integer> getMultipartUploadMaxConcurrency() {
    return Optional.ofNullable(multipartUploadMaxConcurrency);
  }

  public Optional<Long> getMultipartUploadThresholdSizeBytes() {
    return Optional.ofNullable(multipartUploadThresholdSizeBytes);
  }

  public Optional<Integer> getRequestTimeoutSecs() {
    return Optional.ofNullable(requestTimeoutSecs);
  }
}
