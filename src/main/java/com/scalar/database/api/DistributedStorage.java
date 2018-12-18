package com.scalar.database.api;

import com.scalar.database.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;

/**
 * A storage abstraction for interacting with distributed storage implementations. The user can
 * execute CRUD operations on the storage by passing {@link Operation} commands such as {@link Get},
 * {@link Scan}, {@link Put} and {@link Delete}, which specify the location of storage entries and
 * optional information.
 *
 * <p>The data model behind this abstraction is a multi-dimensional map based on the key-value data
 * model. A logical record (or entry) is composed of a partition key, a clustering key and a set of
 * values. The value is uniquely mapped by a primary key composed of partition key, clustering key
 * and value name as described in the following scheme.
 *
 * <p>(partition key, clustering key, value name) {@code =>} value content
 *
 * <p>The physical data model is a multi-dimensional map distributed to multiple nodes by key-based
 * hash partitioning. Entries are assumed to be hash-partitioned by partition key (even though an
 * underlining implementation supports range partitioning). Records with the same partition key,
 * which we call a partition, are sorted by clustering key. Thus, each entry in the storage can be
 * located with the partition key and the clustering key, which we call a primary key. Both a
 * partition key and a clustering key also comprise a list of values. Having a clustering key is
 * optional, and if there is none, the primary key is composed of only the partition key.
 *
 * <h3>Usage Examples</h3>
 *
 * Here is a simple example to demonstrate how to use them. (Exception handling is omitted for
 * readability.)
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
 * props.setProperty(DatabaseConfig.USERNAME, USERNAME);
 * props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
 *
 * // In case of Cassandra storage implementation.
 * DistributedStorage storage = new Cassandra(new DatabaseConfig(props));
 *
 * // Uses KEYSPACE and TABLE by default in this storage instance.
 * storage.with(KEYSPACE, TABLE);
 *
 * // Inserts a new entry which has the primary key value 0 to the storage.
 * // Assumes that the primary key is composed of a integer value named COL_NAME.
 * Put put = new Put(new Key(new IntValue(COL_NAME, 0));
 * storage.put(put);
 *
 * // Retrieves the entry from the storage.
 * Get get = new Get(new Key(new IntValue(COL_NAME, 0));
 * Optional<Result> result = storage.get(get);
 *
 * // Deletes an entry which has the primary key value 1.
 * Delete delete = new Delete(new Key(new IntValue(COL_NAME, 1));
 * storage.delete(delete);
 * }</pre>
 *
 * @author Hiroyuki Yamada
 */
public interface DistributedStorage {
  /**
   * Sets the specified namespace and the table name as a default values in the instance
   *
   * @param namespace default namespace to operate for
   * @param tableName default table name to operate for
   */
  void with(String namespace, String tableName);

  /**
   * Retrieves a result from the storage with the specified {@link Get} command with a primary key
   * and returns the result. The partition keys names and the clustering keys names of the storage
   * should be automatically added to get's projections if it is not specified.
   *
   * @param get a {@code Get} command
   * @return an {@code Optional} with the returned result
   * @throws ExecutionException if the operation failed
   */
  Optional<Result> get(Get get) throws ExecutionException;

  /**
   * Retrieves results from the storage with the specified {@link Scan} command with a partition key
   * and returns {@link Scanner} to iterate the results within the partition. Results can be
   * filtered by specifying a range of clustering keys. The partition keys names and the clustering
   * keys names of the storage should be automatically added to scan's projections if it is not
   * specified.
   *
   * @param scan a {@code Scan} command
   * @return {@link Scanner} to iterate results
   * @throws ExecutionException if the operation failed
   */
  Scanner scan(Scan scan) throws ExecutionException;

  /**
   * Inserts/Updates an entry to the storage with the specified {@link Put} command.
   *
   * @param put a {@code Put} command
   * @throws ExecutionException if the operation failed
   */
  void put(Put put) throws ExecutionException;

  /**
   * Inserts/Updates multiple entries within the same partition to the storage with the specified
   * list of {@link Put} commands. When entries spanning multiple partitions are inserted, this will
   * throw {@code MultiPartitionException}.
   *
   * @param puts a list of {@code Put} commands
   * @throws ExecutionException if the operation failed
   */
  void put(List<Put> puts) throws ExecutionException;

  /**
   * Deletes an entry from the storage with the specified {@link Delete} command.
   *
   * @param delete a {@code Delete} command
   * @throws ExecutionException if the operation failed
   */
  void delete(Delete delete) throws ExecutionException;

  /**
   * Deletes entries from the storage with the specified list of {@link Delete} commands.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws ExecutionException if the operation failed
   */
  void delete(List<Delete> deletes) throws ExecutionException;

  /**
   * Mutates entries of the storage with the specified list of {@link Mutation} commands.
   *
   * @param mutations a list of {@code Mutation} commands
   * @throws ExecutionException if the operation failed
   */
  void mutate(List<? extends Mutation> mutations) throws ExecutionException;

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  void close();
}
