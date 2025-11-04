package com.scalar.db.storage.objectstorage.s3;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Config implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "s3";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";

  private static final Logger logger = LoggerFactory.getLogger(S3Config.class);
  private final String bucket;
  private final String metadataNamespace;
  private final String region;

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

    metadataNamespace = databaseConfig.getSystemNamespaceName();

    if (databaseConfig.getScanFetchSize() != DatabaseConfig.DEFAULT_SCAN_FETCH_SIZE) {
      logger.warn(
          "The configuration property \""
              + DatabaseConfig.SCAN_FETCH_SIZE
              + "\" is not applicable to S3 and will be ignored.");
    }
  }

  @Override
  public String getStorageName() {
    return STORAGE_NAME;
  }

  @Override
  public String getEndpoint() {
    // Not used in S3 adapter
    return null;
  }

  @Override
  public String getUsername() {
    // Not used in S3 adapter
    return null;
  }

  @Override
  public String getPassword() {
    // Not used in S3 adapter
    return null;
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
}
