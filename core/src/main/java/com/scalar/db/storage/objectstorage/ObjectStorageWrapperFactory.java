package com.scalar.db.storage.objectstorage;

import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageConfig;
import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageWrapper;
import java.util.Objects;

public class ObjectStorageWrapperFactory {

  public static ObjectStorageWrapper create(ObjectStorageConfig objectStorageConfig) {
    if (Objects.equals(objectStorageConfig.getStorageName(), BlobStorageConfig.STORAGE_NAME)) {
      assert objectStorageConfig instanceof BlobStorageConfig;
      return new BlobStorageWrapper((BlobStorageConfig) objectStorageConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + objectStorageConfig.getStorageName());
    }
  }
}
