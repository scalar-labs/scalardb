package com.scalar.db.storage.objectstorage.blobstorage;

import com.scalar.db.storage.objectstorage.ObjectStorageProvider;

public class BlobStorageProvider implements ObjectStorageProvider {

  @Override
  public String getName() {
    return BlobStorageConfig.STORAGE_NAME;
  }
}
