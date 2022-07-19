package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** An administrative interface. */
public interface Admin {

  /**
   * Creates a namespace.
   *
   * @param namespace the namespace to create
   * @param options namespace creation options
   * @throws ExecutionException if the operation failed
   */
  void createNamespace(String namespace, Map<String, String> options) throws ExecutionException;

  /**
   * Creates a namespace.
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
   * Creates a namespace.
   *
   * @param namespace the namespace to create
   * @param ifNotExists if set to true, the namespace will be created only if it does not exist
   *     already. If set to false, it will try to create the namespace but may throw an exception if
   *     it already exists
   * @throws ExecutionException if the operation failed
   */
  default void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    createNamespace(namespace, ifNotExists, Collections.emptyMap());
  }

  /**
   * Creates a namespace.
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
   * @param namespace an existing namespace
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
    if (ifNotExists && tableExists(namespace, table)) {
      return;
    }
    createTable(namespace, table, metadata, options);
  }

  /**
   * Creates a new table.
   *
   * @param namespace an existing namespace
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
    createTable(namespace, table, metadata, ifNotExists, Collections.emptyMap());
  }

  /**
   * Creates a new table.
   *
   * @param namespace an existing namespace
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
   * Drops the specified table.
   *
   * @param namespace a namespace to drop
   * @param table a table to drop
   * @param ifExists if set to true, the table will be dropped only if it exists. If set to false,
   *     it will try to drop the table but may throw an exception if it does not exist
   * @throws ExecutionException if the operation failed
   */
  default void dropTable(String namespace, String table, boolean ifExists)
      throws ExecutionException {
    if (ifExists && !tableExists(namespace, table)) {
      return;
    }
    dropTable(namespace, table);
  }

  /**
   * Drops the specified namespace.
   *
   * @param namespace a namespace to drop
   * @throws ExecutionException if the operation failed
   */
  void dropNamespace(String namespace) throws ExecutionException;

  /**
   * Drops the specified namespace.
   *
   * @param namespace a namespace to drop
   * @param ifExists if set to true, the namespace will be dropped only if it exists. If set to
   *     false, it will try to drop the namespace but may throw an exception if it does not exist
   * @throws ExecutionException if the operation failed
   */
  default void dropNamespace(String namespace, boolean ifExists) throws ExecutionException {
    if (ifExists && !namespaceExists(namespace)) {
      return;
    }
    dropNamespace(namespace);
  }

  /**
   * Truncates the specified table.
   *
   * @param namespace a namespace to truncate
   * @param table a table to truncate
   * @throws ExecutionException if the operation failed
   */
  void truncateTable(String namespace, String table) throws ExecutionException;

  /**
   * Creates a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to create a secondary index
   * @param table a table to create a secondary index
   * @param columnName a name of the target column
   * @param options options to create a secondary index
   * @throws ExecutionException if the operation failed
   */
  void createIndex(String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException;

  /**
   * Creates a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to create a secondary index
   * @param table a table to create a secondary index
   * @param columnName a name of the target column
   * @param ifNotExists if set to true, the secondary index will be created only if it does not
   *     exist already. If set to false, it will try to create the secondary index but may throw an
   *     exception if it already exists
   * @param options options to create a secondary index
   * @throws ExecutionException if the operation failed
   */
  default void createIndex(
      String namespace,
      String table,
      String columnName,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    if (ifNotExists && indexExists(namespace, table, columnName)) {
      return;
    }
    createIndex(namespace, table, columnName, options);
  }

  /**
   * Creates a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to create a secondary index
   * @param table a table to create a secondary index
   * @param columnName a name of the target column
   * @param ifNotExists if set to true, the secondary index will be created only if it does not
   *     exist already. If set to false, it will try to create the secondary index but may throw an
   *     exception if it already exists
   * @throws ExecutionException if the operation failed
   */
  default void createIndex(String namespace, String table, String columnName, boolean ifNotExists)
      throws ExecutionException {
    createIndex(namespace, table, columnName, ifNotExists, Collections.emptyMap());
  }

  /**
   * Creates a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to create a secondary index
   * @param table a table to create a secondary index
   * @param columnName a name of the target column
   * @throws ExecutionException if the operation failed
   */
  default void createIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    createIndex(namespace, table, columnName, Collections.emptyMap());
  }

  /**
   * Drops a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to drop a secondary index
   * @param table a table to drop a secondary index
   * @param columnName a name of the target column
   * @throws ExecutionException if the operation failed
   */
  void dropIndex(String namespace, String table, String columnName) throws ExecutionException;

  /**
   * Drops a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to drop a secondary index
   * @param table a table to drop a secondary index
   * @param columnName a name of the target column
   * @param ifExists if set to true, the secondary index will be dropped only if it exists. If set
   *     to false, it will try to drop the secondary index but may throw an exception if it does not
   *     exist
   * @throws ExecutionException if the operation failed
   */
  default void dropIndex(String namespace, String table, String columnName, boolean ifExists)
      throws ExecutionException {
    if (ifExists && !indexExists(namespace, table, columnName)) {
      return;
    }
    dropIndex(namespace, table, columnName);
  }

  /**
   * Returns true if the secondary index exists.
   *
   * @param namespace a namespace
   * @param table a table
   * @param columnName a name of the target column
   * @return true if the secondary index exists, false otherwise
   * @throws ExecutionException if the operation failed
   */
  default boolean indexExists(String namespace, String table, String columnName)
      throws ExecutionException {
    return getTableMetadata(namespace, table).getSecondaryIndexNames().contains(columnName);
  }

  /**
   * Retrieves the table metadata of the specified table.
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return the table metadata of the specified table. null if the table is not found.
   * @throws ExecutionException if the operation failed
   */
  TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException;

  /**
   * Returns the names of the table belonging to the given namespace.
   *
   * @param namespace a namespace
   * @return a set of table names, an empty list if the namespace doesn't exist
   * @throws ExecutionException if the operation failed
   */
  Set<String> getNamespaceTableNames(String namespace) throws ExecutionException;

  /**
   * Returns true if the namespace exists.
   *
   * @param namespace a namespace
   * @return true if the namespace exists, false otherwise
   * @throws ExecutionException if the operation failed
   */
  boolean namespaceExists(String namespace) throws ExecutionException;

  /**
   * Returns true if the table exists.
   *
   * @param namespace a namespace
   * @param table a table
   * @return true if the table exists, false otherwise
   * @throws ExecutionException if the operation failed
   */
  default boolean tableExists(String namespace, String table) throws ExecutionException {
    return getNamespaceTableNames(namespace).contains(table);
  }

  /**
   * Repair a table which may be in an unknown state
   *
   * @param namespace an existing namespace
   * @param table an existing table
   * @param metadata the metadata associated to the table to repair
   * @param options options to repair
   * @throws ExecutionException if the operation failed
   */
  void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException;

  /**
   * Add a new column to an existing table. The new column cannot be a partition or clustering key
   *
   * @param namespace the table namespace
   * @param table the table name
   * @param columnName the name of the new column
   * @param columnType the type of the new column
   * @throws ExecutionException if the operation failed
   */
  void addNewColumnToTable(String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException;
}
