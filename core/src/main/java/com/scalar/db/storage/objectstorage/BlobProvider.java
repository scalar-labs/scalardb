package com.scalar.db.storage.objectstorage;

public class BlobProvider implements ObjectStorageProvider {
  @Override
  public String getName() {
    return BlobConfig.STORAGE_NAME;
  }
}
