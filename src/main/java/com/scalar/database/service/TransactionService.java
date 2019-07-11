package com.scalar.database.service;

import com.google.inject.Inject;
import com.scalar.database.api.DistributedTransaction;
import com.scalar.database.api.DistributedTransactionManager;
import com.scalar.database.api.Isolation;
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
  public void close() {
    manager.close();
  }
}
