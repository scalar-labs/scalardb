package com.scalar.db.storage.objectstorage.blob;

import com.scalar.db.storage.objectstorage.ObjectStorageProvider;

public class BlobProvider implements ObjectStorageProvider {

  @Override
  public String getName() {
    return BlobConfig.STORAGE_NAME;
  }
}
