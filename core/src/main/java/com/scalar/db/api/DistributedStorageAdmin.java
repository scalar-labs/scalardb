package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** An administrative interface for distributed storage implementations. */
public interface DistributedStorageAdmin {

  String INDEX_NAME_PREFIX = "index";

  /**
   * Create a namespace
   *
   * @param namespace the namespace to create
   * @param options namespace creation options
   * @throws ExecutionException if the operation failed
   */
  void createNamespace(String namespace, Map<String, String> options) throws ExecutionException;

  /**
   * Create a namespace
   *
   * @param namespace the namespace to create
   * @param options namespace creation options
   * @param ifNotExists if set to true, the namespace will be created only if it does not exist
   *     already. If set to false, it will try to create the namespace but may throw an exception if
   *     it already exists
   * @throws ExecutionException if the namespace already exists among other
   */
  default void createNamespace(String namespace, boolean ifNotExists, Map<String, String> options)
      throws ExecutionException {
    if (ifNotExists && namespaceExists(namespace)) {
      return;
    }
    createNamespace(namespace, options);
  }

  /**
   * Create a namespace
   *
   * @param namespace the namespace to create
   * @param ifNotExists if set to true, the namespace will be created only if it does not exist
   *     already. If set to false, it will try to create the namespace but may throw an exception if
   *     it already exists
   * @throws ExecutionException if the operation failed
   */
  default void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    if (ifNotExists && namespaceExists(namespace)) {
      return;
    }
    createNamespace(namespace, Collections.emptyMap());
  }

  /**
   * Create a namespace
   *
   * @param namespace the namespace to create
   * @throws ExecutionException if the namespace already exits among other
   */
  default void createNamespace(String namespace) throws ExecutionException {
    createNamespace(namespace, Collections.emptyMap());
  }
  /**
   * Creates a new table.
   *
   * @param namespace a namespace already created
   * @param table a table to create
   * @param metadata a metadata to create
   * @param options options to create
   * @throws ExecutionException if the operation failed
   */
  void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException;

  /**
   * Creates a new table.
   *
   * @param namespace a namespace already created
   * @param table a table to create
   * @param metadata a metadata to create
   * @param ifNotExists if set to true, the table will be created only if it does not exist already.
   *     If set to false, it will try to create the table but may throw an exception if it already
   *     exists
   * @param options options to create
   * @throws ExecutionException if the operation failed
   */
  default void createTable(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    if (ifNotExists && getNamespaceTableNames(namespace).contains(table)) {
      return;
    }
    createTable(namespace, table, metadata, options);
  }

  /**
   * Creates a new table.
   *
   * @param namespace a namespace already created
   * @param table a table to create
   * @param metadata a metadata to create
   * @param ifNotExists if set to true, the table will be created only if it does not exist already.
   *     If set to false, it will try to create the table but may throw an exception if it already
   *     exists
   * @throws ExecutionException if the operation failed
   */
  default void createTable(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    if (ifNotExists && getNamespaceTableNames(namespace).contains(table)) {
      return;
    }
    createTable(namespace, table, metadata, Collections.emptyMap());
  }
  /**
   * Creates a new table.
   *
   * @param namespace a namespace already created
   * @param table a table to create
   * @param metadata a metadata to create
   * @throws ExecutionException if the operation failed
   */
  default void createTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    createTable(namespace, table, metadata, Collections.emptyMap());
  }
  /**
   * Drops the specified table.
   *
   * @param namespace a namespace to drop
   * @param table a table to drop
   * @throws ExecutionException if the operation failed
   */
  void dropTable(String namespace, String table) throws ExecutionException;

  /**
   * Drops the specified namespace.
   *
   * @param namespace a namespace to drop
   * @throws ExecutionException if the operation failed
   */
  void dropNamespace(String namespace) throws ExecutionException;

  /**
   * Truncates the specified table.
   *
   * @param namespace a namespace to truncate
   * @param table a table to truncate
   * @throws ExecutionException if the operation failed
   */
  void truncateTable(String namespace, String table) throws ExecutionException;

  /**
   * Retrieves the table metadata of the specified table
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return the table metadata of the specified table. null if the table is not found.
   * @throws ExecutionException if the operation failed
   */
  TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException;

  /**
   * Returns the names of the table belonging to the given namespace
   *
   * @param namespace a namespace
   * @return a set of table names
   * @throws ExecutionException if the operation failed
   */
  Set<String> getNamespaceTableNames(String namespace) throws ExecutionException;

  /**
   * Return true if the namespace exists
   *
   * @param namespace a namespace
   * @return true if the namespace exists, false otherwise
   * @throws ExecutionException if the operation failed
   */
  boolean namespaceExists(String namespace) throws ExecutionException;

  /** Closes connections to the storage. */
  void close();
}
