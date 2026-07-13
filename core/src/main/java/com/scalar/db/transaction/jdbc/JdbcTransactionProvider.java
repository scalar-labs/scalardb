package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.AbstractDistributedTransactionProvider;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import javax.annotation.Nullable;

public class JdbcTransactionProvider extends AbstractDistributedTransactionProvider {

  @Override
  public String getName() {
    return JdbcConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createRawDistributedTransactionManager(
      DatabaseConfig config) {
    return new JdbcTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new JdbcTransactionAdmin(config);
  }

  @Nullable
  @Override
  public TwoPhaseCommitTransactionManager createRawTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return null;
  }

  @Override
  public TwoPhaseCommit.Coordinator createRawTwoPhaseCommitCoordinator(DatabaseConfig config) {
    throw new UnsupportedOperationException(
        CoreError.JDBC_TRANSACTION_TWO_PHASE_COMMIT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public TwoPhaseCommit.Participant createRawTwoPhaseCommitParticipant(DatabaseConfig config) {
    throw new UnsupportedOperationException(
        CoreError.JDBC_TRANSACTION_TWO_PHASE_COMMIT_NOT_SUPPORTED.buildMessage());
  }
}
