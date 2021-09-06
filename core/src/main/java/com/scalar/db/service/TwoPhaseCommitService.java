package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommitManager;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TwoPhaseCommitService implements TwoPhaseCommitManager {
  private final TwoPhaseCommitManager manager;

  @Inject
  public TwoPhaseCommitService(TwoPhaseCommitManager manager) {
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
  public TwoPhaseCommit start() throws TransactionException {
    return manager.start();
  }

  @Override
  public TwoPhaseCommit start(String txId) throws TransactionException {
    return manager.start(txId);
  }

  @Override
  public TwoPhaseCommit join(String txId) throws TransactionException {
    return manager.join(txId);
  }

  @Override
  public TwoPhaseCommit resume(String txId) throws TransactionException {
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
