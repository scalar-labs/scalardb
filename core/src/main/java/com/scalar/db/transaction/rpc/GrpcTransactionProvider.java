package com.scalar.db.transaction.rpc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;

public class GrpcTransactionProvider implements DistributedTransactionProvider {
  @Override
  public String getName() {
    return "grpc";
  }

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    return new GrpcTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new GrpcTransactionAdmin(config);
  }

  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return new GrpcTwoPhaseCommitTransactionManager(config);
  }
}
