package com.scalar.db.storage.objectstorage;

public interface ObjectStorageConfig {

  /**
   * Returns the storage name.
   *
   * @return the storage name
   */
  String getStorageName();

  /**
   * Returns the password for authentication.
   *
   * @return the password
   */
  String getPassword();

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

  /**
   * Returns the object storage format for partition data serialization.
   *
   * @return the format (default: JSON)
   */
  ObjectStorageFormat getFormat();

  /**
   * Returns whether GZIP compression is enabled for partition data.
   *
   * @return true if compression is enabled (default: false)
   */
  boolean isCompressionEnabled();
}
