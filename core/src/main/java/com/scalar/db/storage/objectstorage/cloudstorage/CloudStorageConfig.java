package com.scalar.db.storage.objectstorage.cloudstorage;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getString;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.ObjectStorageConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudStorageConfig implements ObjectStorageConfig {
  public static final String STORAGE_NAME = "cloud-storage";
  public static final String STORAGE_NAME_IN_PREFIX = "cloud_storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME_IN_PREFIX + ".";
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  public static final String UPLOAD_CHUNK_SIZE_BYTES = PREFIX + "upload_chunk_size_bytes";

  private static final Logger logger = LoggerFactory.getLogger(CloudStorageConfig.class);
  private final String password;
  private final String bucket;
  private final String metadataNamespace;
  private final String projectId;
  private final Integer uploadChunkSizeBytes;

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
              + "\" is not applicable to Cloud Storage and will be ignored.");
    }

    uploadChunkSizeBytes = getInt(databaseConfig.getProperties(), UPLOAD_CHUNK_SIZE_BYTES, null);
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

  public String getProjectId() {
    return projectId;
  }

  public Credentials getCredentials() {
    String serviceAccountJson = getPassword();
    if (serviceAccountJson == null) {
      throw new IllegalArgumentException(
          CoreError.OBJECT_STORAGE_CLOUD_STORAGE_SERVICE_ACCOUNT_KEY_NOT_FOUND.buildMessage());
    }
    try (ByteArrayInputStream keyStream =
        new ByteArrayInputStream(serviceAccountJson.getBytes(StandardCharsets.UTF_8))) {
      return ServiceAccountCredentials.fromStream(keyStream);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          CoreError.OBJECT_STORAGE_CLOUD_STORAGE_SERVICE_ACCOUNT_KEY_LOAD_FAILED.buildMessage());
    }
  }

  public Optional<Integer> getUploadChunkSizeBytes() {
    return Optional.ofNullable(uploadChunkSizeBytes);
  }
}
