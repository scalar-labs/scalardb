package com.scalar.db.transaction.singlecrudoperation;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import javax.annotation.Nullable;

public class SingleCrudOperationTransactionProvider implements DistributedTransactionProvider {

  @Override
  public String getName() {
    return SingleCrudOperationTransactionConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    return new SingleCrudOperationTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new SingleCrudOperationTransactionAdmin(config);
  }

  @Nullable
  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return null;
  }

  @Override
  public TwoPhaseCommitCoordinator createTwoPhaseCommitCoordinator(DatabaseConfig config) {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_TWO_PHASE_COMMIT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public TwoPhaseCommitParticipant createTwoPhaseCommitParticipant(DatabaseConfig config) {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_TWO_PHASE_COMMIT_NOT_SUPPORTED.buildMessage());
  }
}
