package com.scalar.db.server.service;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.server.Metrics;
import com.scalar.db.server.Pauser;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.TransactionModule;

public class ServerModule extends AbstractModule {

  private final ServerConfig config;
  private final Injector storageInjector;
  private final Injector transactionInjector;

  public ServerModule(ServerConfig config, DatabaseConfig databaseConfig) {
    this.config = config;
    storageInjector = Guice.createInjector(new StorageModule(databaseConfig));
    transactionInjector = Guice.createInjector(new TransactionModule(databaseConfig));
  }

  @Provides
  @Singleton
  DistributedStorage provideDistributedStorage() {
    return storageInjector.getInstance(DistributedStorage.class);
  }

  @Provides
  @Singleton
  DistributedStorageAdmin provideDistributedStorageAdmin() {
    return storageInjector.getInstance(DistributedStorageAdmin.class);
  }

  @Provides
  @Singleton
  DistributedTransactionManager provideDistributedTransactionManager() {
    return transactionInjector.getInstance(DistributedTransactionManager.class);
  }

  @Provides
  @Singleton
  Pauser providePauser() {
    return new Pauser();
  }

  @Provides
  @Singleton
  Metrics provideMetrics() {
    return new Metrics(config);
  }
}
