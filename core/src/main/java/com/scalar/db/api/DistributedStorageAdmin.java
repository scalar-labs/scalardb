package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

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

  /**
   * Creates a virtual table that exposes a logical join of two source tables on their primary key.
   * This feature is intended for scenarios where related data is stored in separate tables but
   * needs to be accessed or queried as a single logical entity.
   *
   * <p>Semantics:
   *
   * <ul>
   *   <li>The join is performed on the primary-key columns of both sources, which must share the
   *       same schema (columns, order, and types).
   *   <li>Row set depends on {@code joinType}:
   *       <ul>
   *         <li>{@code INNER}: only keys present in both sources.
   *         <li>{@code LEFT_OUTER}: all keys from the left; for left-only keys, the right-side
   *             columns appear as {@code NULL}.
   *       </ul>
   *   <li>Column order: [primary key columns] + [left non-key columns] + [right non-key columns].
   *   <li>No non-key column name conflicts between sources are allowed.
   *   <li>Both sources must reside within the atomicity unit of the underlying storage, meaning
   *       that the atomicity unit must be at least at the namespace level.
   *   <li>Currently, using virtual tables as sources is not supported.
   * </ul>
   *
   * <p>Note: This feature is primarily for internal use. Breaking changes can and will be
   * introduced to it. Users should not depend on it.
   *
   * @param namespace the namespace of the virtual table to create
   * @param table the name of the virtual table to create
   * @param leftSourceNamespace the namespace of the left source table
   * @param leftSourceTable the name of the left source table
   * @param rightSourceNamespace the namespace of the right source table
   * @param rightSourceTable the name of the right source table
   * @param joinType the type of join to perform between the two source tables
   * @param options additional options for creating the virtual table
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if preconditions are not met (schema mismatch, name conflicts,
   *     unsupported atomicity unit, etc.)
   */
  void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      Map<String, String> options)
      throws ExecutionException;

  /**
   * Creates a virtual table that exposes a logical join of two source tables on their primary key.
   *
   * <p>See {@link #createVirtualTable(String, String, String, String, String, String,
   * VirtualTableJoinType, Map)} for semantics.
   *
   * <p>Note: This feature is primarily for internal use. Breaking changes can and will be
   * introduced to it. Users should not depend on it.
   *
   * @param namespace the namespace of the virtual table to create
   * @param table the name of the virtual table to create
   * @param leftSourceNamespace the namespace of the left source table
   * @param leftSourceTable the name of the left source table
   * @param rightSourceNamespace the namespace of the right source table
   * @param rightSourceTable the name of the right source table
   * @param joinType the type of join to perform between the two source tables
   * @param ifNotExists if set to true, the virtual table will be created only if it does not exist
   *     already. If set to false, it will throw an exception if it already exists
   * @param options additional options for creating the virtual table
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if preconditions are not met (schema mismatch, name conflicts,
   *     unsupported atomicity unit, etc.)
   */
  default void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    if (ifNotExists && tableExists(namespace, table)) {
      return;
    }
    createVirtualTable(
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        options);
  }

  /**
   * Creates a virtual table that exposes a logical join of two source tables on their primary key.
   *
   * <p>See {@link #createVirtualTable(String, String, String, String, String, String,
   * VirtualTableJoinType, Map)} for semantics.
   *
   * <p>Note: This feature is primarily for internal use. Breaking changes can and will be
   * introduced to it. Users should not depend on it.
   *
   * @param namespace the namespace of the virtual table to create
   * @param table the name of the virtual table to create
   * @param leftSourceNamespace the namespace of the left source table
   * @param leftSourceTable the name of the left source table
   * @param rightSourceNamespace the namespace of the right source table
   * @param rightSourceTable the name of the right source table
   * @param joinType the type of join to perform between the two source tables
   * @param ifNotExists if set to true, the virtual table will be created only if it does not exist
   *     already. If set to false, it will throw an exception if it already exists
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if preconditions are not met (schema mismatch, name conflicts,
   *     unsupported atomicity unit, etc.)
   */
  default void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      boolean ifNotExists)
      throws ExecutionException {
    createVirtualTable(
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        ifNotExists,
        Collections.emptyMap());
  }

  /**
   * Creates a virtual table that exposes a logical join of two source tables on their primary key.
   *
   * <p>See {@link #createVirtualTable(String, String, String, String, String, String,
   * VirtualTableJoinType, Map)} for semantics.
   *
   * <p>Note: This feature is primarily for internal use. Breaking changes can and will be
   * introduced to it. Users should not depend on it.
   *
   * @param namespace the namespace of the virtual table to create
   * @param table the name of the virtual table to create
   * @param leftSourceNamespace the namespace of the left source table
   * @param leftSourceTable the name of the left source table
   * @param rightSourceNamespace the namespace of the right source table
   * @param rightSourceTable the name of the right source table
   * @param joinType the type of join to perform between the two source tables
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if preconditions are not met (schema mismatch, name conflicts,
   *     unsupported atomicity unit, etc.)
   */
  default void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType)
      throws ExecutionException {
    createVirtualTable(
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        Collections.emptyMap());
  }

  /**
   * Returns the virtual table information.
   *
   * <p>Note: This feature is primarily for internal use. Breaking changes can and will be
   * introduced to it. Users should not depend on it.
   *
   * @param namespace the namespace
   * @param table the table
   * @return the virtual table information or {@code Optional.empty()} if the table is not a virtual
   *     table
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if the table does not exist
   */
  Optional<VirtualTableInfo> getVirtualTableInfo(String namespace, String table)
      throws ExecutionException;

  /** Closes connections to the storage. */
  @Override
  void close();
}
