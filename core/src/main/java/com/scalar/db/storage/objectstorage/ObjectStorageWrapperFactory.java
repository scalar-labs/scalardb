package com.scalar.db.storage.objectstorage;

import com.scalar.db.storage.objectstorage.blob.BlobConfig;
import com.scalar.db.storage.objectstorage.blob.BlobWrapper;
import com.scalar.db.storage.objectstorage.s3.S3Config;
import com.scalar.db.storage.objectstorage.s3.S3Wrapper;
import java.util.Objects;

public class ObjectStorageWrapperFactory {

  public static ObjectStorageWrapper create(ObjectStorageConfig objectStorageConfig) {
    if (Objects.equals(objectStorageConfig.getStorageName(), BlobConfig.STORAGE_NAME)) {
      assert objectStorageConfig instanceof BlobConfig;
      return new BlobWrapper((BlobConfig) objectStorageConfig);
    } else if (Objects.equals(objectStorageConfig.getStorageName(), S3Config.STORAGE_NAME)) {
      assert objectStorageConfig instanceof S3Config;
      return new S3Wrapper((S3Config) objectStorageConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + objectStorageConfig.getStorageName());
    }
  }
}
