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
   * @throws ConflictOccurredException if the object keeps being updated by concurrent writes while
   *     it is read
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
   * @throws PreconditionFailedException if the object already exists, in which case the object has
   *     not been inserted
   * @throws ConflictOccurredException if the insertion conflicts with a concurrent write, in which
   *     case the object has not been inserted
   * @throws ObjectStorageWrapperException if an error occurs, including when the object may have
   *     been inserted, for example because the client resent the request after a lost response
   */
  void insert(String key, String object) throws ObjectStorageWrapperException;

  /**
   * Update the object in the storage if the version matches.
   *
   * @param key the key of the object
   * @param object the updated object
   * @param version the expected version of the object
   * @throws PreconditionFailedException if the version does not match or the object does not exist,
   *     in which case the object has not been updated
   * @throws ConflictOccurredException if the update conflicts with a concurrent write, in which
   *     case the object has not been updated
   * @throws ObjectStorageWrapperException if an error occurs, including when the object may have
   *     been updated, for example because the client resent the request after a lost response
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
   * @throws PreconditionFailedException if the version does not match or the object does not exist,
   *     in which case the object has not been deleted
   * @throws ConflictOccurredException if the deletion conflicts with a concurrent write, in which
   *     case the object has not been deleted
   * @throws ObjectStorageWrapperException if an error occurs, including when the object may have
   *     been deleted, for example because the client resent the request after a lost response
   */
  void delete(String key, String version) throws ObjectStorageWrapperException;

  /**
   * Delete objects with the specified prefix from the storage. <br>
   * <br>
   * <strong>Attention:</strong> This method does not guarantee atomicity and is assumed to be used
   * where concurrent operations do not occur.
   *
   * @param prefix the prefix of the objects to delete
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void deleteByPrefix(String prefix) throws ObjectStorageWrapperException;

  /**
   * Close the storage wrapper.
   *
   * @throws ObjectStorageWrapperException if an error occurs
   */
  void close() throws ObjectStorageWrapperException;
}
