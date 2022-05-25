package com.scalar.db.server;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;

public class ServerModule extends AbstractModule {

  private final ServerConfig config;
  private final DatabaseConfig databaseConfig;
  private final StorageFactory storageFactory;
  private final TransactionFactory transactionFactory;

  public ServerModule(ServerConfig config) {
    this.config = config;
    this.databaseConfig = new DatabaseConfig(config.getProperties());
    storageFactory = new StorageFactory(databaseConfig);
    transactionFactory = new TransactionFactory(databaseConfig);
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
  DistributedTransactionAdmin provideDistributedTransactionAdmin() {
    return transactionFactory.getTransactionAdmin();
  }

  @Provides
  @Singleton
  Metrics provideMetrics() {
    return new Metrics(config);
  }

  @Provides
  @Singleton
  TableMetadataManager provideTableMetadataManager(DistributedStorageAdmin admin) {
    return new TableMetadataManager(admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
  }
}
