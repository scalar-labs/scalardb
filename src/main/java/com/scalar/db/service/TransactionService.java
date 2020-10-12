package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class TransactionService implements DistributedTransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionService.class);
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
  public void withTableName(String tableName) {
    manager.withTableName(tableName);
  }

  @Override
  public DistributedTransaction start() {
    return manager.start();
  }

  @Override
  public DistributedTransaction start(String txId) {
    return manager.start(txId);
  }

  @Override
  public DistributedTransaction start(Isolation isolation) {
    return manager.start(isolation);
  }

  @Override
  public DistributedTransaction start(String txId, Isolation isolation) {
    return manager.start(txId, isolation);
  }

  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy) {
    return manager.start(isolation, strategy);
  }

  @Override
  public DistributedTransaction start(SerializableStrategy strategy) {
    return manager.start(strategy);
  }

  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy) {
    return manager.start(txId, strategy);
  }

  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) {
    return null;
  }

  @Override
  public TransactionState getState(String txId) {
    return manager.getState(txId);
  }

  @Override
  public void close() {
    manager.close();
  }
}
