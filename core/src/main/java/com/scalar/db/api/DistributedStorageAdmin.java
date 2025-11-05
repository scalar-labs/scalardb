package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;

/**
 * An administrative interface for distributed storage implementations. The user can execute
 * administrative operations with it like createNamespace/createTable/getTableMetadata.
 *
 * <h3>Usage Examples</h3>
 *
 * Here is a simple example to demonstrate how to use it. (Exception handling is omitted for
 * readability.)
 *
 * <pre>{@code
 * StorageFactory factory = StorageFactory.create(configFilePath);
 * DistributedStorageAdmin admin = factory.getAdmin();
 *
 * // Create a namespace
 * // Assumes that the namespace name is NAMESPACE
 * admin.createNamespace(NAMESPACE);
 *
 * // Create a table
 * // Assumes that the namespace name and the table name are NAMESPACE and TABLE respectively
 * TableMetadata tableMetadata = TableMetadata.newBuilder()
 *     ...
 *     .build();
 * admin.createTable(NAMESPACE, TABLE, tableMetadata);
 *
 * // Get a table metadata
 * tableMetadata = admin.getTableMetadata(NAMESPACE, TABLE);
 *
 * // Truncate a table
 * admin.truncateTable(NAMESPACE, TABLE);
 *
 * // Drop a table
 * admin.dropTable(NAMESPACE, TABLE);
 *
 * // Drop a namespace
 * admin.dropNamespace(NAMESPACE);
 * }</pre>
 */
public interface DistributedStorageAdmin extends Admin, AutoCloseable {

  /**
   * Returns the storage information.
   *
   * @param namespace the namespace to get the storage information for
   * @return the storage information
   * @throws ExecutionException if the operation fails
   */
  StorageInfo getStorageInfo(String namespace) throws ExecutionException;

  /** Closes connections to the storage. */
  @Override
  void close();
}
