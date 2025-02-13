package com.scalar.db.dataloader.core.dataimport.task;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Result;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Optional;

/**
 * An import task that operates within a {@link DistributedTransaction}.
 *
 * <p>This class extends {@link ImportTask} and overrides methods to fetch and store records using a
 * transactional context.
 */
public class ImportTransactionalTask extends ImportTask {

  private final DistributedTransaction transaction;

  /**
   * Constructs an {@code ImportTransactionalTask} with the specified parameters and transaction.
   *
   * @param params the import task parameters
   * @param transaction the distributed transaction to be used for data operations
   */
  public ImportTransactionalTask(ImportTaskParams params, DistributedTransaction transaction) {
    super(params);
    this.transaction = transaction;
  }

  /**
   * Retrieves a data record within the active transaction.
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
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, transaction);
  }

  /**
   * Saves a record within the active transaction.
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
    params.getDao().put(namespace, tableName, partitionKey, clusteringKey, columns, transaction);
  }

  /**
   * Aborts the active ScalarDB transaction if it has not been committed.
   *
   * @param tx the transaction to be aborted
   * @throws TransactionException if an error occurs during the aborting process
   */
  private void abortActiveTransaction(DistributedTransaction tx) throws TransactionException {
    if (tx != null) {
      try {
        tx.abort();
      } catch (AbortException e) {
        throw new TransactionException(e.getMessage(), tx.getId());
      }
    }
  }
}
