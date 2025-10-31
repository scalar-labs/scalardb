package com.scalar.db.storage.objectstorage;

import com.scalar.db.storage.objectstorage.blob.BlobConfig;
import com.scalar.db.storage.objectstorage.blob.BlobWrapper;
import java.util.Objects;

public class ObjectStorageWrapperFactory {

  public static ObjectStorageWrapper create(ObjectStorageConfig objectStorageConfig) {
    if (Objects.equals(objectStorageConfig.getStorageName(), BlobConfig.STORAGE_NAME)) {
      assert objectStorageConfig instanceof BlobConfig;
      return new BlobWrapper((BlobConfig) objectStorageConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + objectStorageConfig.getStorageName());
    }
  }
}
