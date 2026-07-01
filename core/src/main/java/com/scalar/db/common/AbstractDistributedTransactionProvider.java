package com.scalar.db.common;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
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

  @Override
  public TwoPhaseCommit.Coordinator createTwoPhaseCommitCoordinator(DatabaseConfig config) {
    TwoPhaseCommit.Coordinator coordinator = createRawTwoPhaseCommitCoordinator(config);

    if (config.isActiveTransactionManagementEnabled()) {
      // Wrap the coordinator for active transaction management. This must be the outermost wrapping
      // so that the idle-expiry reap traverses every inner decorator via releaseContext.
      coordinator =
          new ActiveTransactionManagedTwoPhaseCommitCoordinator(
              coordinator, config.getActiveTransactionManagementExpirationTimeMillis());
    }

    return coordinator;
  }

  protected abstract TwoPhaseCommit.Coordinator createRawTwoPhaseCommitCoordinator(
      DatabaseConfig config);

  @Override
  public TwoPhaseCommit.Participant createTwoPhaseCommitParticipant(DatabaseConfig config) {
    TwoPhaseCommit.Participant participant = createRawTwoPhaseCommitParticipant(config);

    if (config.isAttributePropagationEnabled()) {
      // Wrap the participant for transaction-scoped attribute propagation
      participant = new AttributePropagatingTwoPhaseCommitParticipant(participant);
    }

    if (config.isActiveTransactionManagementEnabled()) {
      // Wrap the participant for active transaction management. This must be the outermost wrapping
      // so that the idle-expiry reap traverses every inner decorator via releaseContext.
      participant =
          new ActiveTransactionManagedTwoPhaseCommitParticipant(
              participant, config.getActiveTransactionManagementExpirationTimeMillis());
    }

    return participant;
  }

  protected abstract TwoPhaseCommit.Participant createRawTwoPhaseCommitParticipant(
      DatabaseConfig config);
}
