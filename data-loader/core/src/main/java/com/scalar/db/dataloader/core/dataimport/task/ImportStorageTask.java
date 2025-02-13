package com.scalar.db.dataloader.core.dataimport.task;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Optional;

/**
 * An import task that interacts with a {@link DistributedStorage} for data retrieval and storage.
 *
 * <p>This class extends {@link ImportTask} and overrides methods to fetch and store records using
 * the provided {@code DistributedStorage} instance.
 */
public class ImportStorageTask extends ImportTask {

  private final DistributedStorage storage;

  /**
   * Constructs an {@code ImportStorageTask} with the specified parameters and storage.
   *
   * @param params the import task parameters
   * @param storage the distributed storage to be used for data operations
   */
  public ImportStorageTask(ImportTaskParams params, DistributedStorage storage) {
    super(params);
    this.storage = storage;
  }

  /**
   * Retrieves a data record from the distributed storage.
   *
   * @param namespace the namespace of the table
   * @param tableName the name of the table
   * @param partitionKey the partition key of the record
   * @param clusteringKey the clustering key of the record
   * @return an {@link Optional} containing the {@link Result} if the record exists, otherwise an
   *     empty {@link Optional}
   * @throws ScalarDBDaoException if an error occurs during retrieval
   */
  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, this.storage);
  }

  /**
   * Saves a record into the distributed storage.
   *
   * @param namespace the namespace of the table
   * @param tableName the name of the table
   * @param partitionKey the partition key of the record
   * @param clusteringKey the clustering key of the record
   * @param columns the list of columns to be saved
   * @throws ScalarDBDaoException if an error occurs during the save operation
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
