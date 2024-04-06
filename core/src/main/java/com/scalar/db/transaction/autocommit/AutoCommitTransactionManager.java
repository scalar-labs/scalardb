package com.scalar.db.transaction.autocommit;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.StorageFactory;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class AutoCommitTransactionManager
    extends ActiveTransactionManagedDistributedTransactionManager {

  private final DistributedStorage storage;

  public AutoCommitTransactionManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    storage = storageFactory.getStorage();
  }

  @VisibleForTesting
  AutoCommitTransactionManager(DatabaseConfig databaseConfig, DistributedStorage storage) {
    super(databaseConfig);
    this.storage = storage;
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    String txId = UUID.randomUUID().toString();
    return begin(txId);
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    AutoCommitTransaction transaction = new AutoCommitTransaction(txId, storage);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return decorate(transaction);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return begin(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return begin(txId);
  }

  @Override
  public TransactionState getState(String txId) {
    throw new UnsupportedOperationException(
        CoreError.AUTO_COMMIT_TRANSACTION_GETTING_TRANSACTION_STATE_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public TransactionState rollback(String txId) {
    throw new UnsupportedOperationException(
        CoreError.AUTO_COMMIT_TRANSACTION_ROLLING_BACK_TRANSACTION_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void close() {
    storage.close();
  }
}
