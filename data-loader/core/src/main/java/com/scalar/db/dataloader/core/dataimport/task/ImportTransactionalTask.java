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
 * An import task that operates within a {@link DistributedTransaction} context.
 *
 * <p>This class extends {@link ImportTask} and provides transactional semantics for data import
 * operations. It ensures that all data operations (get and put) are executed within the same
 * transaction context, maintaining ACID properties.
 *
 * <p>The task uses a single {@link DistributedTransaction} instance throughout its lifecycle, which
 * is passed during construction. This transaction must be managed (committed or aborted) by the
 * caller.
 */
public class ImportTransactionalTask extends ImportTask {

  private final DistributedTransaction transaction;

  /**
   * Constructs an {@code ImportTransactionalTask} with the specified parameters and transaction.
   *
   * @param params the import task parameters containing configuration and DAO objects
   * @param transaction the distributed transaction to be used for all data operations. This
   *     transaction should be properly managed (committed/aborted) by the caller
   */
  public ImportTransactionalTask(ImportTaskParams params, DistributedTransaction transaction) {
    super(params);
    this.transaction = transaction;
  }

  /**
   * Retrieves a data record within the active transaction context.
   *
   * <p>This method overrides the base implementation to ensure the get operation is executed within
   * the transaction context provided during construction.
   *
   * @param namespace the namespace of the table to query
   * @param tableName the name of the table to query
   * @param partitionKey the partition key identifying the record's partition
   * @param clusteringKey the clustering key for further record identification within the partition
   * @return an {@link Optional} containing the {@link Result} if the record exists, otherwise an
   *     empty {@link Optional}
   * @throws ScalarDBDaoException if an error occurs during the database operation or if the
   *     transaction encounters any issues
   */
  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, transaction);
  }

  /**
   * Saves a record within the active transaction context.
   *
   * <p>This method overrides the base implementation to ensure the put operation is executed within
   * the transaction context provided during construction.
   *
   * @param namespace the namespace of the target table
   * @param tableName the name of the target table
   * @param partitionKey the partition key determining where the record will be stored
   * @param clusteringKey the clustering key for ordering/organizing records within the partition
   * @param columns the list of columns containing the actual data to be saved
   * @throws ScalarDBDaoException if an error occurs during the database operation or if the
   *     transaction encounters any issues
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
   * <p>This method provides a safe way to abort an active transaction, handling any abort-related
   * exceptions by wrapping them in a {@link TransactionException}.
   *
   * @param tx the transaction to be aborted. If null, this method does nothing
   * @throws TransactionException if an error occurs during the abort operation or if the underlying
   *     abort operation fails
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
