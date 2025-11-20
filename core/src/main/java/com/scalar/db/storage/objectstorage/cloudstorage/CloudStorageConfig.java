package com.scalar.db.storage.objectstorage.cloudstorage;

import static com.scalar.db.config.ConfigUtils.getInt;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudStorageConfig implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "cloud-storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";

  public static final String PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES =
      PREFIX + "parallel_upload_block_size_in_bytes";

  private static final Logger logger = LoggerFactory.getLogger(CloudStorageConfig.class);
  private final String bucket;
  private final String metadataNamespace;
  private final String projectId;
  private final Integer parallelUploadBlockSizeInBytes;

  public CloudStorageConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }
    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException("Contact points are not specified.");
    }
    bucket = databaseConfig.getContactPoints().get(0);
    projectId = databaseConfig.getUsername().orElse(null);
    metadataNamespace = databaseConfig.getSystemNamespaceName();

    if (databaseConfig.getScanFetchSize() != DatabaseConfig.DEFAULT_SCAN_FETCH_SIZE) {
      logger.warn(
          "The configuration property \""
              + DatabaseConfig.SCAN_FETCH_SIZE
              + "\" is not applicable to Cloud Storage and will be ignored.");
    }

    parallelUploadBlockSizeInBytes =
        getInt(databaseConfig.getProperties(), PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES, null);
  }

  @Override
  public String getStorageName() {
    return STORAGE_NAME;
  }

  @Override
  public String getBucket() {
    return bucket;
  }

  @Override
  public String getMetadataNamespace() {
    return metadataNamespace;
  }

  public String getProjectId() {
    return projectId;
  }

  public Optional<Integer> getParallelUploadBlockSizeInBytes() {
    return Optional.ofNullable(parallelUploadBlockSizeInBytes);
  }
}
