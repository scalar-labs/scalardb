package com.scalar.db.storage.objectstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.blob.BlobConfig;
import com.scalar.db.storage.objectstorage.blob.BlobWrapper;
import java.util.Objects;

public class ObjectStorageUtils {
  public static final char OBJECT_KEY_DELIMITER = '/';
  public static final char CONCATENATED_KEY_DELIMITER = '!';

  public static String getObjectKey(String namespace, String table, String partition) {
    return String.join(String.valueOf(OBJECT_KEY_DELIMITER), namespace, table, partition);
  }

  public static String getObjectKey(String namespace, String table) {
    return String.join(String.valueOf(OBJECT_KEY_DELIMITER), namespace, table);
  }

  public static ObjectStorageConfig getObjectStorageConfig(DatabaseConfig databaseConfig) {
    if (Objects.equals(databaseConfig.getStorage(), BlobConfig.STORAGE_NAME)) {
      return new BlobConfig(databaseConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + databaseConfig.getStorage());
    }
  }

  public static ObjectStorageWrapper getObjectStorageWrapper(
      ObjectStorageConfig objectStorageConfig) {
    if (Objects.equals(objectStorageConfig.getStorageName(), BlobConfig.STORAGE_NAME)) {
      assert objectStorageConfig instanceof BlobConfig;
      return new BlobWrapper(
          buildBlobContainerClient(objectStorageConfig), (BlobConfig) objectStorageConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + objectStorageConfig.getStorageName());
    }
  }

  private static BlobContainerClient buildBlobContainerClient(
      ObjectStorageConfig objectStorageConfig) {
    return new BlobServiceClientBuilder()
        .endpoint(objectStorageConfig.getEndpoint())
        .credential(
            new StorageSharedKeyCredential(
                objectStorageConfig.getUsername(), objectStorageConfig.getPassword()))
        .buildClient()
        .getBlobContainerClient(objectStorageConfig.getBucket());
  }
}
