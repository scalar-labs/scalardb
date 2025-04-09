package com.scalar.db.dataloader.core.dataimport.task;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Optional;

/**
 * An import task that interacts with a {@link DistributedStorage} for data retrieval and storage
 * operations.
 *
 * <p>This class extends {@link ImportTask} and provides concrete implementations for fetching and
 * storing records using a {@link DistributedStorage} instance. It acts as a bridge between the
 * import process and the underlying distributed storage system.
 *
 * <p>The task handles both read and write operations:
 *
 * <ul>
 *   <li>Reading existing records using partition and clustering keys
 *   <li>Storing new or updated records with their associated columns
 * </ul>
 *
 * <p>All storage operations are performed through the provided {@link DistributedStorage} instance,
 * which must be properly initialized before creating this task.
 */
public class ImportStorageTask extends ImportTask {

  private final DistributedStorage storage;

  /**
   * Constructs an {@code ImportStorageTask} with the specified parameters and storage.
   *
   * @param params the import task parameters containing configuration and DAO objects
   * @param storage the distributed storage instance to be used for data operations
   * @throws NullPointerException if either params or storage is null
   */
  public ImportStorageTask(ImportTaskParams params, DistributedStorage storage) {
    super(params);
    this.storage = storage;
  }

  /**
   * Retrieves a data record from the distributed storage using the specified keys.
   *
   * <p>This method attempts to fetch a single record from the specified table using both partition
   * and clustering keys. The operation is performed through the configured DAO using the associated
   * storage instance.
   *
   * @param namespace the namespace of the table to query
   * @param tableName the name of the table to query
   * @param partitionKey the partition key identifying the record's partition
   * @param clusteringKey the clustering key for further record identification within the partition
   * @return an {@link Optional} containing the {@link Result} if the record exists, otherwise an
   *     empty {@link Optional}
   * @throws ScalarDBDaoException if an error occurs during the retrieval operation, such as
   *     connection issues or invalid table/namespace
   */
  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, this.storage);
  }

  /**
   * Saves a record into the distributed storage with the specified keys and columns.
   *
   * <p>This method writes or updates a record in the specified table using the provided keys and
   * column values. The operation is performed through the configured DAO using the associated
   * storage instance.
   *
   * @param namespace the namespace of the target table
   * @param tableName the name of the target table
   * @param partitionKey the partition key determining where the record will be stored
   * @param clusteringKey the clustering key for organizing records within the partition
   * @param columns the list of columns containing the record's data to be saved
   * @throws ScalarDBDaoException if an error occurs during the save operation, such as connection
   *     issues, invalid data types, or constraint violations
   */
  @Override
  protected void saveRecord(
      String namespace,
      String tableName,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns)
      throws ScalarDBDaoException {
    params.getDao().put(namespace, tableName, partitionKey, clusteringKey, columns, this.storage);
  }
}
