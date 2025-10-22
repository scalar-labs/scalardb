package com.scalar.db.storage.objectstorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.blob.BlobConfig;
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
}
