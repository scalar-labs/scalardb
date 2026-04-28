package com.scalar.db.api;

import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.common.AttributePropagatingDistributedTransactionManager;
import com.scalar.db.common.StateManagedDistributedTransactionManager;
import com.scalar.db.common.StateManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import javax.annotation.Nullable;

public abstract class AbstractDistributedTransactionProvider
    implements DistributedTransactionProvider {

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    DistributedTransactionManager transactionManager =
        createRawDistributedTransactionManager(config);

    // Wrap the transaction manager for state management
    transactionManager = new StateManagedDistributedTransactionManager(transactionManager);

    if (config.isAttributePropagationEnabled()) {
      // Wrap the transaction manager for transaction-scoped attribute propagation
      transactionManager =
          new AttributePropagatingDistributedTransactionManager(transactionManager);
    }

    if (config.isActiveTransactionManagementEnabled()) {
      // Wrap the transaction manager for active transaction management. This must be the
      // outermost wrapping so that transactions returned by resume / join (which come from the
      // active transaction registry) carry the behavior of every inner decorator.
      transactionManager =
          new ActiveTransactionManagedDistributedTransactionManager(
              transactionManager, config.getActiveTransactionManagementExpirationTimeMillis());
    }

    return transactionManager;
  }

  protected abstract DistributedTransactionManager createRawDistributedTransactionManager(
      DatabaseConfig config);

  @Nullable
  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    TwoPhaseCommitTransactionManager transactionManager =
        createRawTwoPhaseCommitTransactionManager(config);

    if (transactionManager == null) {
      return null;
    }

    // Wrap the transaction manager for state management
    transactionManager = new StateManagedTwoPhaseCommitTransactionManager(transactionManager);

    if (config.isActiveTransactionManagementEnabled()) {
      // Wrap the transaction manager for active transaction management
      transactionManager =
          new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
              transactionManager, config.getActiveTransactionManagementExpirationTimeMillis());
    }

    return transactionManager;
  }

  protected abstract TwoPhaseCommitTransactionManager createRawTwoPhaseCommitTransactionManager(
      DatabaseConfig config);
}
