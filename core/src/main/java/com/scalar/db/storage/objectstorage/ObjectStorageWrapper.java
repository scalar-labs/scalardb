package com.scalar.db.storage.objectstorage;

import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface ObjectStorageWrapper {

  /**
   * Get the object from the storage.
   *
   * @param key the key of the object
   * @throws ObjectStorageWrapperException if an error occurs
   * @return the object and its version wrapped in an Optional if found, otherwise an empty Optional
   */
  Optional<ObjectStorageWrapperResponse> get(String key) throws ObjectStorageWrapperException;

  /**
   * Get object keys with the specified prefix.
   *
   * @param prefix the prefix of the keys
   * @throws ObjectStorageWrapperException if an error occurs
   * @return the set of keys with the specified prefix
   */
  Set<String> getKeys(String prefix) throws ObjectStorageWrapperException;

  /**
   * Insert the object into the storage.
   *
   * @param key the key of the object
   * @param object the object to insert
   * @throws PreconditionFailedException if the object already exists
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void insert(String key, String object) throws ObjectStorageWrapperException;

  /**
   * Update the object in the storage if the version matches.
   *
   * @param key the key of the object
   * @param object the updated object
   * @param version the expected version of the object
   * @throws PreconditionFailedException if the version does not match or the object does not exist
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void update(String key, String object, String version) throws ObjectStorageWrapperException;

  /**
   * Delete the object from the storage.
   *
   * @param key the key of the object
   * @throws PreconditionFailedException if the object does not exist
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void delete(String key) throws ObjectStorageWrapperException;

  /**
   * Delete the object from the storage if the version matches.
   *
   * @param key the key of the object
   * @param version the expected version of the object
   * @throws PreconditionFailedException if the version does not match or the object does not exist
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void delete(String key, String version) throws ObjectStorageWrapperException;

  /**
   * Delete objects with the specified prefix from the storage.
   *
   * @param prefix the prefix of the objects to delete
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void deleteByPrefix(String prefix) throws ObjectStorageWrapperException;

  /** Close the storage wrapper. */
  void close() throws ObjectStorageWrapperException;
}
