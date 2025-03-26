package com.scalar.db.storage.objectstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.scalar.db.config.DatabaseConfig;
import java.util.Objects;
import javax.annotation.Nullable;

public class ObjectStorageUtils {
  public static final char OBJECT_KEY_DELIMITER = '/';
  public static final char CONCATENATED_KEY_DELIMITER = '*';

  public static String getObjectKey(String namespace, String table, @Nullable String partition) {
    if (partition == null) {
      return String.join(String.valueOf(OBJECT_KEY_DELIMITER), namespace, table);
    } else {
      return String.join(String.valueOf(OBJECT_KEY_DELIMITER), namespace, table, partition);
    }
  }

  public static ObjectStorageConfig getObjectStorageConfig(DatabaseConfig databaseConfig) {
    if (Objects.equals(databaseConfig.getStorage(), BlobConfig.STORAGE_NAME)) {
      return new BlobConfig(databaseConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported object storage: " + databaseConfig.getStorage());
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
          "Unsupported object storage: " + objectStorageConfig.getStorageName());
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
