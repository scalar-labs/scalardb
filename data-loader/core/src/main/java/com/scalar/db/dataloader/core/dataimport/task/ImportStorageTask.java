package com.scalar.db.dataloader.core.dataimport.task;

import com.scalar.db.api.Result;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDaoException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import java.util.List;
import java.util.Optional;

/**
 * An import task that performs data retrieval and storage operations using a {@link
 * SingleCrudOperationTransactionManager}.
 *
 * <p>This class extends {@link ImportTask} and provides concrete implementations for fetching and
 * saving records through the associated DAO using the {@link
 * SingleCrudOperationTransactionManager}. It serves as a bridge between the data import process and
 * the underlying single CRUD operation layer of ScalarDB.
 *
 * <p>The task handles two types of operations:
 *
 * <ul>
 *   <li>Reading existing records using partition and clustering keys.
 *   <li>Writing or updating records with their corresponding columns.
 * </ul>
 *
 * <p>All operations are executed via the configured DAO, which internally uses the provided {@link
 * SingleCrudOperationTransactionManager} instance.
 */
public class ImportStorageTask extends ImportTask {

  private final SingleCrudOperationTransactionManager manager;

  /**
   * Constructs an {@code ImportStorageTask} with the specified parameters and {@link
   * SingleCrudOperationTransactionManager}.
   *
   * @param params the import task parameters containing configuration and DAO instances
   * @param manager the {@code SingleCrudOperationTransactionManager} used for data operations
   * @throws NullPointerException if {@code params} or {@code manager} is {@code null}
   */
  public ImportStorageTask(ImportTaskParams params, SingleCrudOperationTransactionManager manager) {
    super(params);
    this.manager = manager;
  }

  /**
   * Retrieves a data record using the provided keys.
   *
   * <p>This method attempts to fetch a single record from the specified table using the given
   * partition and clustering keys. The operation is performed through the configured DAO using the
   * associated {@link SingleCrudOperationTransactionManager}.
   *
   * @param namespace the namespace of the table to query
   * @param tableName the name of the table to query
   * @param partitionKey the partition key identifying the record's partition
   * @param clusteringKey the clustering key identifying the record within the partition
   * @return an {@link Optional} containing the {@link Result} if the record exists; otherwise, an
   *     empty {@link Optional}
   * @throws ScalarDbDaoException if an error occurs during the retrieval operation, such as
   *     connectivity issues or invalid schema references
   */
  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDbDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, this.manager);
  }

  /**
   * Saves or updates a record in the specified table using the provided keys and column values.
   *
   * <p>This method inserts or updates a record in the target table using the {@link
   * SingleCrudOperationTransactionManager}. It is typically invoked during the data import process
   * to persist new or modified records.
   *
   * @param namespace the namespace of the target table
   * @param tableName the name of the target table
   * @param partitionKey the partition key identifying where the record is stored
   * @param clusteringKey the clustering key specifying the record's ordering within the partition
   * @param columns the list of columns representing the record data to save
   * @throws ScalarDbDaoException if an error occurs during the save operation, such as connection
   *     issues or constraint violations
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
