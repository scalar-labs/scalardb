package com.scalar.db.storage.objectstorage;

public interface ObjectStorageConfig {

  /**
   * Returns the storage name.
   *
   * @return the storage name
   */
  String getStorageName();

  /**
   * Returns the bucket name.
   *
   * @return the bucket name
   */
  String getBucket();

  /**
   * Returns the metadata namespace.
   *
   * @return the metadata namespace
   */
  String getMetadataNamespace();
}
