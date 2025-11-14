package com.scalar.db.storage.objectstorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageConfig;
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

  public static String[] parseObjectKey(String objectKey) {
    return objectKey.split(String.valueOf(OBJECT_KEY_DELIMITER), 3);
  }

  public static ObjectStorageConfig getObjectStorageConfig(DatabaseConfig databaseConfig) {
    if (Objects.equals(databaseConfig.getStorage(), BlobStorageConfig.STORAGE_NAME)) {
      return new BlobStorageConfig(databaseConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + databaseConfig.getStorage());
    }
  }
}
