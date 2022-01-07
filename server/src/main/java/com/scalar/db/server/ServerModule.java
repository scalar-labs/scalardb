package com.scalar.db.server;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.util.Properties;

public class ServerModule extends AbstractModule {

  private final ServerConfig config;
  private final StorageFactory storageFactory;
  private final TransactionFactory transactionFactory;

  public ServerModule(ServerConfig config, DatabaseConfig databaseConfig) {
    this.config = config;
    storageFactory = new StorageFactory(databaseConfig);

    Properties transactionProperties = new Properties();
    transactionProperties.putAll(databaseConfig.getProperties());

    // For two-phase consensus commit transactions in Scalar DB server, disable the active
    // transactions management because Scalar DB server takes care of active transactions management
    if (databaseConfig.getTransactionManagerClass() == ConsensusCommitManager.class) {
      transactionProperties.put(
          ConsensusCommitConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "false");
    } else if (databaseConfig.getTransactionManagerClass() == GrpcTransactionManager.class) {
      transactionProperties.put(GrpcConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "false");
    }
    transactionFactory = new TransactionFactory(new DatabaseConfig(transactionProperties));
  }

  @Override
  protected void configure() {
    bind(GateKeeper.class).to(config.getGateKeeperClass()).in(Singleton.class);
  }

  @Provides
  @Singleton
  DistributedStorage provideDistributedStorage() {
    return storageFactory.getStorage();
  }

  @Provides
  @Singleton
  DistributedStorageAdmin provideDistributedStorageAdmin() {
    return storageFactory.getAdmin();
  }

  @Provides
  @Singleton
  DistributedTransactionManager provideDistributedTransactionManager() {
    return transactionFactory.getTransactionManager();
  }

  @Provides
  @Singleton
  TwoPhaseCommitTransactionManager provideTwoPhaseCommitTransactionManager() {
    return transactionFactory.getTwoPhaseCommitTransactionManager();
  }

  @Provides
  @Singleton
  Metrics provideMetrics() {
    return new Metrics(config);
  }
}
