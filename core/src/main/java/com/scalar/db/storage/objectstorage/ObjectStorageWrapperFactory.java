package com.scalar.db.storage.objectstorage;

import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageConfig;
import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageWrapper;
import com.scalar.db.storage.objectstorage.cloudstorage.CloudStorageConfig;
import com.scalar.db.storage.objectstorage.cloudstorage.CloudStorageWrapper;
import com.scalar.db.storage.objectstorage.s3.S3Config;
import com.scalar.db.storage.objectstorage.s3.S3Wrapper;
import java.util.Objects;

public class ObjectStorageWrapperFactory {

  public static ObjectStorageWrapper create(ObjectStorageConfig objectStorageConfig) {
    if (Objects.equals(objectStorageConfig.getStorageName(), BlobStorageConfig.STORAGE_NAME)) {
      assert objectStorageConfig instanceof BlobStorageConfig;
      return new BlobStorageWrapper((BlobStorageConfig) objectStorageConfig);
    } else if (Objects.equals(objectStorageConfig.getStorageName(), S3Config.STORAGE_NAME)) {
      assert objectStorageConfig instanceof S3Config;
      return new S3Wrapper((S3Config) objectStorageConfig);
    } else if (Objects.equals(
        objectStorageConfig.getStorageName(), CloudStorageConfig.STORAGE_NAME)) {
      assert objectStorageConfig instanceof CloudStorageConfig;
      return new CloudStorageWrapper((CloudStorageConfig) objectStorageConfig);
    } else {
      throw new IllegalArgumentException(
          "Unsupported Object Storage: " + objectStorageConfig.getStorageName());
    }
  }
}
