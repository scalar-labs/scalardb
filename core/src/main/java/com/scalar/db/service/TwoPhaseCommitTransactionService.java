package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

/** @deprecated As of release 3.5.0. Will be removed in release 5.0.0 */
@Deprecated
@Immutable
public class TwoPhaseCommitTransactionService implements TwoPhaseCommitTransactionManager {
  private final TwoPhaseCommitTransactionManager manager;

  @Inject
  public TwoPhaseCommitTransactionService(TwoPhaseCommitTransactionManager manager) {
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
  public TwoPhaseCommitTransaction start() throws TransactionException {
    return manager.start();
  }

  @Override
  public TwoPhaseCommitTransaction start(String txId) throws TransactionException {
    return manager.start(txId);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    return manager.join(txId);
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) throws TransactionException {
    return manager.resume(txId);
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
