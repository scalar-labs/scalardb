package com.scalar.db.storage.objectstorage.cloudstorage;

import com.scalar.db.storage.objectstorage.ObjectStorageProvider;
import com.scalar.db.storage.objectstorage.s3.S3Config;

public class CloudStorageProvider implements ObjectStorageProvider {

  @Override
  public String getName() {
    return S3Config.STORAGE_NAME;
  }
}
