package com.scalar.db.storage.objectstorage.s3;

import com.scalar.db.storage.objectstorage.ObjectStorageProvider;

public class S3Provider implements ObjectStorageProvider {

  @Override
  public String getName() {
    return S3Config.STORAGE_NAME;
  }
}
