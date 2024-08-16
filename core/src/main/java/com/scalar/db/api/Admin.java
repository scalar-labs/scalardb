package com.scalar.db.api;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/** An administrative interface. */
public interface Admin {

  /**
   * Creates a namespace.
   *
   * @param namespace the namespace to create
   * @param options namespace creation options
   * @throws IllegalArgumentException if the namespace already exists
   * @throws ExecutionException if the operation fails
   */
  void createNamespace(String namespace, Map<String, String> options) throws ExecutionException;

  /**
   * Creates a namespace.
   *
   * @param namespace the namespace to create
   * @param ifNotExists if set to true, the namespace will be created only if it does not exist
   *     already. If set to false, it will throw an exception if it already exists
   * @param options namespace creation options
   * @throws IllegalArgumentException if the namespace already exists if ifNotExists is set to false
   * @throws ExecutionException if the operation fails
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
   *     already. If set to false, it will throw an exception if it already exists
   * @throws IllegalArgumentException if the namespace already exists if ifNotExists is set to false
   * @throws ExecutionException if the operation fails
   */
  default void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    createNamespace(namespace, ifNotExists, Collections.emptyMap());
  }

  /**
   * Creates a namespace.
   *
   * @param namespace the namespace to create
   * @throws IllegalArgumentException if the namespace already exists
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the namespace does not exist or the table already exists
   * @throws ExecutionException if the operation fails
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
   *     If set to false, it will throw an exception if it already exists
   * @param options options to create
   * @throws IllegalArgumentException if the namespace does not exist. Or if the table already
   *     exists if ifNotExists is set to false
   * @throws ExecutionException if the operation fails
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
   *     If set to false, it will throw an exception if it already exists
   * @throws IllegalArgumentException if the namespace does not exist. Or if the table already
   *     exists if ifNotExists is set to false
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the namespace does not exist or the table already exists
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the table does not exist
   * @throws ExecutionException if the operation fails
   */
  void dropTable(String namespace, String table) throws ExecutionException;

  /**
   * Drops the specified table.
   *
   * @param namespace a namespace to drop
   * @param table a table to drop
   * @param ifExists if set to true, the table will be dropped only if it exists. If set to false,
   *     it will throw an exception if it does not exist
   * @throws IllegalArgumentException if the table does not exist if ifExists is set to false
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the namespace does not exist or the namespace is not empty
   * @throws ExecutionException if the operation fails
   */
  void dropNamespace(String namespace) throws ExecutionException;

  /**
   * Drops the specified namespace.
   *
   * @param namespace a namespace to drop
   * @param ifExists if set to true, the namespace will be dropped only if it exists. If set to
   *     false, it will throw an exception if it does not exist
   * @throws IllegalArgumentException if the namespace does not exist if ifExists is set to false.
   *     Or if the namespace is not empty
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the table does not exist
   * @throws ExecutionException if the operation fails
   */
  void truncateTable(String namespace, String table) throws ExecutionException;

  /**
   * Creates a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to create a secondary index
   * @param table a table to create a secondary index
   * @param columnName a name of the target column
   * @param options options to create a secondary index
   * @throws IllegalArgumentException if the table does not exist or the column does not exist or
   *     the index already exists
   * @throws ExecutionException if the operation fails
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
   *     exist already. If set to false, it will throw an exception if it already exists
   * @param options options to create a secondary index
   * @throws IllegalArgumentException if the table does not exist or the column does not exist. Or
   *     the index already exists if ifNotExists is set to false
   * @throws ExecutionException if the operation fails
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
   *     exist already. If set to false, it will throw an exception if it already exists
   * @throws IllegalArgumentException if the table does not exist or the column does not exist. Or
   *     the index already exists if ifNotExists is set to false
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the table does not exist or the column does not exist or
   *     the index already exists
   * @throws ExecutionException if the operation fails
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
   * @throws IllegalArgumentException if the table does not exist or the index does not exist
   * @throws ExecutionException if the operation fails
   */
  void dropIndex(String namespace, String table, String columnName) throws ExecutionException;

  /**
   * Drops a secondary index for the specified column of the specified table.
   *
   * @param namespace a namespace to drop a secondary index
   * @param table a table to drop a secondary index
   * @param columnName a name of the target column
   * @param ifExists if set to true, the secondary index will be dropped only if it exists. If set
   *     to false, it will throw an exception if it does not exist
   * @throws IllegalArgumentException if the table does not exist. Or if the index does not exist if
   *     ifExists is set to false
   * @throws ExecutionException if the operation fails
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
   * @throws ExecutionException if the operation fails
   */
  default boolean indexExists(String namespace, String table, String columnName)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      return false;
    }
    return tableMetadata.getSecondaryIndexNames().contains(columnName);
  }

  /**
   * Retrieves the table metadata of the specified table.
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return the table metadata of the specified table. null if the table is not found.
   * @throws ExecutionException if the operation fails
   */
  @Nullable
  TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException;

  /**
   * Returns the names of the table belonging to the given namespace.
   *
   * @param namespace a namespace
   * @return a set of table names, an empty set if the namespace doesn't exist
   * @throws ExecutionException if the operation fails
   */
  Set<String> getNamespaceTableNames(String namespace) throws ExecutionException;

  /**
   * Returns true if the namespace exists.
   *
   * @param namespace a namespace
   * @return true if the namespace exists, false otherwise
   * @throws ExecutionException if the operation fails
   */
  boolean namespaceExists(String namespace) throws ExecutionException;

  /**
   * Returns true if the table exists.
   *
   * @param namespace a namespace
   * @param table a table
   * @return true if the table exists, false otherwise
   * @throws ExecutionException if the operation fails
   */
  default boolean tableExists(String namespace, String table) throws ExecutionException {
    return getNamespaceTableNames(namespace).contains(table);
  }

  /**
   * Repairs a namespace that may be in an unknown state, such as the namespace exists in the
   * underlying storage but not its ScalarDB metadata or vice versa. This will re-create the
   * namespace and its metadata if necessary.
   *
   * @param namespace a namespace
   * @param options options to repair
   * @throws ExecutionException if the operation fails
   */
  void repairNamespace(String namespace, Map<String, String> options) throws ExecutionException;

  /**
   * Repairs a table that may be in an unknown state, such as the table exists in the underlying
   * storage but not its ScalarDB metadata or vice versa. This will re-create the table, its
   * secondary indexes, and their metadata if necessary.
   *
   * @param namespace a namespace
   * @param table a table
   * @param metadata the metadata associated to the table to repair
   * @param options options to repair
   * @throws ExecutionException if the operation fails
   */
  void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException;

  /**
   * Adds a new column to an existing table. The new column cannot be a partition or clustering key.
   * <br>
   * <br>
   * <strong>Attention:</strong> You should carefully consider adding a new column to a table
   * because the execution time may vary greatly depending on the underlying storage. Please plan
   * accordingly and consider the following, especially if the database runs in production:
   *
   * <ul>
   *   <li><strong>For Cosmos DB for noSQL and DynamoDB:</strong> Adding a column is almost
   *       instantaneous as the table schema is not modified. Only the table metadata stored in a
   *       separate table is updated.
   *   <li><strong>For Cassandra:</strong> Adding a column will only update the schema metadata and
   *       will not modify the existing schema records. The cluster topology is the main factor for
   *       the execution time. Changes to the schema metadata are shared to each cluster node via a
   *       gossip protocol. Because of this, the larger the cluster, the longer it will take for all
   *       nodes to be updated.
   *   <li><strong>For relational databases (MySQL, Oracle, etc.):</strong> Adding a column
   *       shouldn't take a long time to execute.
   * </ul>
   *
   * @param namespace the table namespace
   * @param table the table name
   * @param columnName the name of the new column
   * @param columnType the type of the new column
   * @throws IllegalArgumentException if the table does not exist or the column already exists
   * @throws ExecutionException if the operation fails
   */
  void addNewColumnToTable(String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException;

  /**
   * Adds a new column to an existing table. The new column cannot be a partition or clustering key.
   * <br>
   * See {@link #addNewColumnToTable(String, String, String, DataType)} for more information.
   *
   * @param namespace the table namespace
   * @param table the table name
   * @param columnName the name of the new column
   * @param columnType the type of the new column
   * @param encrypted whether the new column should be encrypted
   * @throws IllegalArgumentException if the table does not exist or the column already exists
   * @throws ExecutionException if the operation fails
   */
  default void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType, boolean encrypted)
      throws ExecutionException {
    if (encrypted) {
      throw new UnsupportedOperationException(
          CoreError.TRANSPARENT_DATA_ENCRYPTION_NOT_ENABLED.buildMessage());
    } else {
      addNewColumnToTable(namespace, table, columnName, columnType);
    }
  }

  /**
   * Imports an existing table that is not managed by ScalarDB.
   *
   * @param namespace an existing namespace
   * @param table an existing table
   * @param options options to import
   * @throws IllegalArgumentException if the table is already managed by ScalarDB, if the target
   *     table does not exist, or if the table does not meet the requirement of ScalarDB table
   * @throws ExecutionException if the operation fails
   */
  void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException;

  /**
   * Returns the names of the existing namespaces created through Scalar DB.
   *
   * @return a set of namespaces names, an empty set if no namespaces exist
   * @throws ExecutionException if the operation fails
   */
  Set<String> getNamespaceNames() throws ExecutionException;

  /**
   * Upgrades the ScalarDB environment to support the latest version of the ScalarDB API. Typically,
   * as indicated in the release notes, you will need to run this method after updating the ScalarDB
   * version that your application environment uses.
   *
   * @param options options to upgrade
   * @throws ExecutionException if the operation fails
   */
  void upgrade(Map<String, String> options) throws ExecutionException;
}
