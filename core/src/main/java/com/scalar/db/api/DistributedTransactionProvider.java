package com.scalar.db.api;

import com.scalar.db.config.DatabaseConfig;
import javax.annotation.Nullable;

/**
 * A class that creates {@link DistributedTransactionManager}, {@link DistributedTransactionAdmin},
 * and {@link TwoPhaseCommitTransactionManager} instances. Each transaction manager should implement
 * this class to instantiate its implementations of {@link DistributedTransactionManager}, {@link
 * DistributedTransactionAdmin}, and {@link TwoPhaseCommitTransactionManager}. The implementations
 * are assumed to be loaded by {@link java.util.ServiceLoader}.
 */
public interface DistributedTransactionProvider {

  /**
   * Returns the name of the adapter. This is for the configuration {@link
   * DatabaseConfig#TRANSACTION_MANAGER}.
   *
   * @return the name of the adapter
   */
  String getName();

  /**
   * Creates an instance of {@link DistributedTransactionManager} for the transaction manager.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionManager} for the transaction manager
   */
  DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config);

  /**
   * Creates an instance of {@link DistributedTransactionAdmin} for the transaction manager.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionAdmin} for the transaction manager
   */
  DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config);

  /**
   * Creates an instance of {@link TwoPhaseCommitTransactionManager} for the transaction manager.
   *
   * @param config a database config
   * @return an instance of {@link TwoPhaseCommitTransactionManager} for the transaction manager. If
   *     the transaction manager does not support the two-phase commit interface, returns {@code
   *     null}.
   */
  @Nullable
  TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(DatabaseConfig config);

  /**
   * Creates an instance of {@link TwoPhaseCommitCoordinator} for the new multi-participant
   * two-phase commit interface.
   *
   * @param config a database config
   * @return an instance of {@link TwoPhaseCommitCoordinator}
   * @throws UnsupportedOperationException if the transaction manager does not support the two-phase
   *     commit interface
   */
  TwoPhaseCommitCoordinator createTwoPhaseCommitCoordinator(DatabaseConfig config);

  /**
   * Creates an instance of {@link TwoPhaseCommitParticipant} for the new multi-participant
   * two-phase commit interface.
   *
   * @param config a database config
   * @return an instance of {@link TwoPhaseCommitParticipant}
   * @throws UnsupportedOperationException if the transaction manager does not support the two-phase
   *     commit interface
   */
  TwoPhaseCommitParticipant createTwoPhaseCommitParticipant(DatabaseConfig config);

  /**
   * Creates an instance of {@link GlobalTransactionManager}.
   *
   * @param config a database config
   * @return an instance of {@link GlobalTransactionManager}
   * @throws UnsupportedOperationException if the transaction manager does not support global
   *     transactions
   */
  GlobalTransactionManager createGlobalTransactionManager(DatabaseConfig config);
}
