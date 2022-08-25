package com.scalar.db.transaction.rpc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionFactory;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;

public class GrpcTransactionFactory implements DistributedTransactionFactory {
  @Override
  public String getName() {
    return "grpc";
  }

  @Override
  public DistributedTransactionManager getDistributedTransactionManager(DatabaseConfig config) {
    return new GrpcTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin getDistributedTransactionAdmin(DatabaseConfig config) {
    return new GrpcTransactionAdmin(config);
  }

  @Override
  public TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return new GrpcTwoPhaseCommitTransactionManager(config);
  }
}
