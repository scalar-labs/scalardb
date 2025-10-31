package com.scalar.db.storage.objectstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.scalar.db.storage.objectstorage.blob.BlobConfig;
import com.scalar.db.storage.objectstorage.blob.BlobWrapper;
import java.util.Objects;

public class ObjectStorageWrapperFactory {

  public static ObjectStorageWrapper create(ObjectStorageConfig objectStorageConfig) {
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
