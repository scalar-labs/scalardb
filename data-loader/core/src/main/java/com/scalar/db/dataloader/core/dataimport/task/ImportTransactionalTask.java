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

public class ImportTransactionalTask extends ImportTask {

  private final DistributedTransaction transaction;

  public ImportTransactionalTask(ImportTaskParams params, DistributedTransaction transaction) {
    super(params);
    this.transaction = transaction;
  }

  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, transaction);
  }

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
   * Abort the active ScalarDB transaction
   *
   * @throws TransactionException if something goes wrong during the aborting process
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
