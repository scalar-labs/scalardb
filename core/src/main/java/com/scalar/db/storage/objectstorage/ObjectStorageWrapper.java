package com.scalar.db.storage.objectstorage;

import java.util.Set;

public interface ObjectStorageWrapper {

  /**
   * Get the object from the storage.
   *
   * @param key the key of the object
   * @throws ObjectStorageWrapperException if the object does not exist
   * @return the object and its version
   */
  ObjectStorageWrapperResponse get(String key) throws ObjectStorageWrapperException;

  /**
   * Get object keys with the specified prefix.
   *
   * @param prefix the prefix of the keys
   * @return the set of keys with the specified prefix
   */
  Set<String> getKeys(String prefix);

  /**
   * Insert the object into the storage.
   *
   * @param key the key of the object
   * @param object the object to insert
   * @throws ObjectStorageWrapperException if the object already exists or a conflict occurs
   */
  void insert(String key, String object) throws ObjectStorageWrapperException;

  /**
   * Update the object in the storage if the version matches.
   *
   * @param key the key of the object
   * @param object the updated object
   * @param version the expected version of the object
   * @return true if the object is updated, false otherwise
   */
  boolean updateIfVersionMatches(String key, String object, String version);

  /**
   * Delete the object from the storage.
   *
   * @param key the key of the object
   * @throws ObjectStorageWrapperException if the object does not exist or a conflict occurs
   */
  void delete(String key) throws ObjectStorageWrapperException;

  /**
   * Delete the object from the storage if the version matches.
   *
   * @param key the key of the object
   * @param version the expected version of the object
   * @return true if the object is deleted, false otherwise
   */
  boolean deleteIfVersionMatches(String key, String version);

  /** Close the storage wrapper. */
  void close();
}
