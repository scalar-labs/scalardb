package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;

/**
 * A storage abstraction for interacting with distributed storage implementations. The user can
 * execute CRUD operations to the storage by passing {@link Operation} commands such as {@link Get},
 * {@link Scan}, {@link Put} and {@link Delete}, which specify the location of storage entries and
 * optional information.
 *
 * <p>The data model behind this abstraction is a multi-dimensional map based on key-value data
 * model. A logical record (or entry) is composed of partition key, clustering key and a set of
 * columns. The column value is uniquely mapped by a primary key composed of partition key,
 * clustering key and column name as described in the following scheme.
 *
 * <p>(partition key, clustering key, column name) {@code =>} column value
 *
 * <p>The physical data model is a multi-dimensional map distributed to multiple nodes by key-based
 * hash partitioning. Entries are assumed to be hash-partitioned by partition key (even though an
 * underlying implementation supports range partitioning). Records with the same partition key,
 * which we call a partition, are sorted by clustering key. Thus, each entry in the storage can be
 * located with the partition key and the clustering key, which we call it primary key. Both a
 * partition key and a clustering key also comprise a list of values. Having clustering key is
 * optional, so in that case, primary key is composed of only partition key.
 *
 * <h3>Usage Examples</h3>
 *
 * Here is a simple example to demonstrate how to use them. (Exception handling is omitted for
 * readability.)
 *
 * <pre>{@code
 * StorageFactory factory = StorageFactory.create(configFilePath);
 * DistributedStorage storage = factory.getStorage();
 *
 * // Inserts a new entry which has the primary key value 0 to the storage.
 * // Assumes that the namespace name and the table name are NAMESPACE and TABLE respectively, and
 * // the primary key is composed of a integer column named COL_NAME.
 * Put put =
 *     Put.newBuilder()
 *         .namespace(NAMESPACE)
 *         .table(TABLE)
 *         .partitionKey(Key.ofInt(COL_NAME, 0))
 *         ...
 *         .build();
 * storage.put(put);
 *
 * // Retrieves the entry from the storage.
 * Get get =
 *     Get.newBuilder()
 *         .namespace(NAMESPACE)
 *         .table(TABLE)
 *         .partitionKey(Key.ofInt(COL_NAME, 0))
 *         .build();
 * Optional<Result> result = storage.get(get);
 *
 * // Deletes an entry which has the primary key value 1.
 * Delete delete =
 *     Delete.newBuilder()
 *         .namespace(NAMESPACE)
 *         .table(TABLE)
 *         .partitionKey(Key.ofInt(COL_NAME, 0))
 *         .build();
 * storage.delete(delete);
 * }</pre>
 *
 * @author Hiroyuki Yamada
 */
public interface DistributedStorage {
  /**
   * Sets the specified namespace and the table name as default values in the instance.
   *
   * @param namespace default namespace to operate for
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void with(String namespace, String tableName);

  /**
   * Sets the specified namespace as a default value in the instance.
   *
   * @param namespace default namespace to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void withNamespace(String namespace);

  /**
   * Returns the namespace.
   *
   * @return an {@code Optional} with the namespace
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<String> getNamespace();

  /**
   * Sets the specified table name as a default value in the instance.
   *
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void withTable(String tableName);

  /**
   * Returns the table name.
   *
   * @return an {@code Optional} with the table name
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<String> getTable();

  /**
   * Retrieves a result from the storage with the specified {@link Get} command with a primary key
   * and returns the result.
   *
   * @param get a {@code Get} command
   * @return an {@code Optional} with the returned result
   * @throws ExecutionException if the operation fails
   */
  Optional<Result> get(Get get) throws ExecutionException;

  /**
   * Retrieves results from the underlying storage with the specified {@link Scan} or {@link ScanAll} or {@link
   * ScanWithIndex} command and returns {@link Scanner} to iterate the results.
   *
   * <ul>
   *   <li>{@link Scan} : by specifying a partition key, it will return results within the
   *       partition. Results can be filtered by specifying a range of clustering keys.
   *   <li>{@link ScanAll} : for a given table, it will return all its records even if they span
   *       several partitions.
   *   <li>{@link ScanWithIndex} : by specifying an index key, it will return results within the
   *       index.
   * </ul>
   *
   * @param scan a {@code Scan} or {@code ScanAll} command
   * @return {@link Scanner} to iterate results
   * @throws ExecutionException if the operation fails
   */
  Scanner scan(Scan scan) throws ExecutionException;

  /**
   * Inserts/Updates an entry to the storage with the specified {@link Put} command.
   *
   * @param put a {@code Put} command
   * @throws ExecutionException if the operation fails
   */
  void put(Put put) throws ExecutionException;

  /**
   * Inserts/Updates multiple entries within the same partition to the storage with the specified
   * list of {@link Put} commands. When entries spanning multiple partitions are inserted, this will
   * throw {@code MultiPartitionException}.
   *
   * @param puts a list of {@code Put} commands
   * @throws ExecutionException if the operation fails
   */
  void put(List<Put> puts) throws ExecutionException;

  /**
   * Deletes an entry from the storage with the specified {@link Delete} command.
   *
   * @param delete a {@code Delete} command
   * @throws ExecutionException if the operation fails
   */
  void delete(Delete delete) throws ExecutionException;

  /**
   * Deletes entries from the storage with the specified list of {@link Delete} commands.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws ExecutionException if the operation fails
   */
  void delete(List<Delete> deletes) throws ExecutionException;

  /**
   * Mutates entries of the storage with the specified list of {@link Mutation} commands.
   *
   * @param mutations a list of {@code Mutation} commands
   * @throws ExecutionException if the operation fails
   */
  void mutate(List<? extends Mutation> mutations) throws ExecutionException;

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  void close();
}
