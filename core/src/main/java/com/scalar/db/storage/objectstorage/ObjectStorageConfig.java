package com.scalar.db.storage.objectstorage;

public interface ObjectStorageConfig {
  /**
   * Returns the storage name.
   *
   * @return the storage name
   */
  String getStorageName();

  /**
   * Returns the endpoint for the object storage service.
   *
   * @return the endpoint
   */
  String getEndpoint();

  /**
   * Returns the username for authentication.
   *
   * @return the username
   */
  String getUsername();

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
}
