package com.scalar.db.dataloader.core.dataimport.task;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDaoException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Optional;

/**
 * An import task that performs data operations using a {@link DistributedTransactionManager}.
 *
 * <p>This class extends {@link ImportTask} and provides concrete implementations for retrieving and
 * storing records using a {@link DistributedTransactionManager} instance. It serves as a bridge
 * between the import process and the underlying transactional data management layer in ScalarDB.
 *
 * <p>The task supports both read and write operations:
 *
 * <ul>
 *   <li>Retrieving existing records using partition and optional clustering keys.
 *   <li>Inserting or updating records with their associated column values.
 * </ul>
 *
 * <p>All data operations are delegated to the DAO configured within {@link ImportTaskParams},
 * executed through the provided {@link DistributedTransactionManager}. The manager must be properly
 * initialized and connected before creating an instance of this class.
 */
public class ImportStorageTask extends ImportTask {

  private final DistributedTransactionManager manager;

  /**
   * Constructs an {@code ImportStorageTask} with the specified parameters and transaction manager.
   *
   * @param params the import task parameters containing configuration and DAO objects
   * @param manager the {@link DistributedTransactionManager} instance used for transactional data
   *     operations
   * @throws NullPointerException if either {@code params} or {@code manager} is {@code null}
   */
  public ImportStorageTask(ImportTaskParams params, DistributedTransactionManager manager) {
    super(params);
    this.manager = manager;
  }

  /**
   * Retrieves a data record from the database using the specified keys.
   *
   * <p>This method attempts to fetch a single record from the given table using the provided
   * partition and optional clustering keys. The retrieval is executed through the DAO configured in
   * {@link ImportTaskParams}, utilizing the associated {@link DistributedTransactionManager}.
   *
   * @param namespace the ScalarDB namespace of the target table
   * @param tableName the name of the target table
   * @param partitionKey the partition key identifying the record's partition
   * @param clusteringKey the clustering key for uniquely identifying the record within the
   *     partition
   * @return an {@link Optional} containing the {@link Result} if a record exists, or an empty
   *     {@link Optional} if no matching record is found
   * @throws ScalarDbDaoException if an error occurs during the retrieval operation, such as a
   *     connection failure or invalid table/namespace
   */
  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDbDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, this.manager);
  }

  /**
   * Saves or updates a record in the database using the specified keys and column values.
   *
   * <p>This method writes or updates a record in the specified table using the provided keys and
   * columns. The operation is performed through the DAO configured in {@link ImportTaskParams},
   * utilizing the associated {@link DistributedTransactionManager}.
   *
   * @param namespace the ScalarDB namespace of the target table
   * @param tableName the name of the target table
   * @param partitionKey the partition key determining where the record will be stored
   * @param clusteringKey the clustering key for uniquely identifying the record within the
   *     partition
   * @param columns the list of columns containing the data to be stored
   * @throws ScalarDbDaoException if an error occurs during the save operation, such as connection
   *     issues, invalid data types, or constraint violations
   */
  @Override
  protected void saveRecord(
      String namespace,
      String tableName,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns)
      throws ScalarDbDaoException {
    params.getDao().put(namespace, tableName, partitionKey, clusteringKey, columns, this.manager);
  }
}
