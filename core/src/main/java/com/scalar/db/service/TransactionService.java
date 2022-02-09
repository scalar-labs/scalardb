package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/** @deprecated As of release 3.5.0. Will be removed in release 5.0.0 */
@Deprecated
@ThreadSafe
public class TransactionService implements DistributedTransactionManager {
  private final DistributedTransactionManager manager;

  @Inject
  public TransactionService(DistributedTransactionManager manager) {
    this.manager = manager;
  }

  @Override
  public void with(String namespace, String tableName) {
    manager.with(namespace, tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    manager.withNamespace(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return manager.getNamespace();
  }

  @Override
  public void withTable(String tableName) {
    manager.withTable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return manager.getTable();
  }

  @Override
  public DistributedTransaction start() throws TransactionException {
    return manager.start();
  }

  @Override
  public DistributedTransaction start(String txId) throws TransactionException {
    return manager.start(txId);
  }

  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return manager.start(isolation);
  }

  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return manager.start(txId, isolation);
  }

  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return manager.start(isolation, strategy);
  }

  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return manager.start(strategy);
  }

  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return manager.start(txId, strategy);
  }

  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return manager.start(txId, isolation, strategy);
  }

  @Override
  public TransactionState getState(String txId) throws TransactionException {
    return manager.getState(txId);
  }

  @Override
  public TransactionState abort(String txId) throws TransactionException {
    return manager.abort(txId);
  }

  @Override
  public void close() {
    manager.close();
  }
}
