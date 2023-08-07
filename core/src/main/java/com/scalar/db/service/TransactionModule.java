package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageAdmin;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcStorage;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitManager;
import com.scalar.db.transaction.jdbc.JdbcTransactionAdmin;
import com.scalar.db.transaction.jdbc.JdbcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTransactionAdmin;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransactionManager;

/** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
@Deprecated
public class TransactionModule extends AbstractModule {

  private final DatabaseConfig config;

  public TransactionModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    Class<? extends DistributedStorage> storageClass;
    Class<? extends DistributedStorageAdmin> storageAdminClass;
    switch (config.getStorage().toLowerCase()) {
      case "cassandra":
        storageClass = Cassandra.class;
        storageAdminClass = CassandraAdmin.class;
        break;
      case "cosmos":
        storageClass = Cosmos.class;
        storageAdminClass = CosmosAdmin.class;
        break;
      case "dynamo":
        storageClass = Dynamo.class;
        storageAdminClass = DynamoAdmin.class;
        break;
      case "jdbc":
        storageClass = JdbcDatabase.class;
        storageAdminClass = JdbcAdmin.class;
        break;
      case "multi-storage":
        storageClass = MultiStorage.class;
        storageAdminClass = MultiStorageAdmin.class;
        break;
      case "grpc":
        storageClass = GrpcStorage.class;
        storageAdminClass = GrpcAdmin.class;
        break;
      default:
        throw new IllegalArgumentException("Storage '" + config.getStorage() + "' isn't supported");
    }

    bind(DistributedStorage.class).to(storageClass);
    bind(DistributedStorageAdmin.class).to(storageAdminClass);

    Class<? extends DistributedTransactionManager> transactionManagerClass;
    Class<? extends DistributedTransactionAdmin> transactionAdminClass;
    Class<? extends TwoPhaseCommitTransactionManager> twoPhaseCommitTransactionManagerClass;
    switch (config.getTransactionManager().toLowerCase()) {
      case "consensus-commit":
        transactionManagerClass = ConsensusCommitManager.class;
        transactionAdminClass = ConsensusCommitAdmin.class;
        twoPhaseCommitTransactionManagerClass = TwoPhaseConsensusCommitManager.class;
        break;
      case "jdbc":
        transactionManagerClass = JdbcTransactionManager.class;
        transactionAdminClass = JdbcTransactionAdmin.class;
        twoPhaseCommitTransactionManagerClass = null;
        break;
      case "grpc":
        transactionManagerClass = GrpcTransactionManager.class;
        transactionAdminClass = GrpcTransactionAdmin.class;
        twoPhaseCommitTransactionManagerClass = GrpcTwoPhaseCommitTransactionManager.class;
        break;
      default:
        throw new IllegalArgumentException(
            "Transaction manager '" + config.getTransactionManager() + "' isn't supported");
    }

    bind(DistributedTransactionManager.class).to(transactionManagerClass);
    bind(DistributedTransactionAdmin.class).to(transactionAdminClass);
    if (twoPhaseCommitTransactionManagerClass != null) {
      bind(TwoPhaseCommitTransactionManager.class).to(twoPhaseCommitTransactionManagerClass);
    }
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }
}
