package com.scalar.db.storage.objectstorage.cloudstorage;

import com.scalar.db.storage.objectstorage.ObjectStorageProvider;

public class CloudStorageProvider implements ObjectStorageProvider {

  @Override
  public String getName() {
    return CloudStorageConfig.STORAGE_NAME;
  }
}
