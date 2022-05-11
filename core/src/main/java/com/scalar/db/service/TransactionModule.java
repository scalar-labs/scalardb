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

public class TransactionModule extends AbstractModule {

  private final DatabaseConfig config;

  public TransactionModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(DistributedStorage.class).to(config.getStorageClass()).in(Singleton.class);
    bind(DistributedStorageAdmin.class).to(config.getStorageAdminClass()).in(Singleton.class);

    bind(DistributedTransactionManager.class)
        .to(config.getTransactionManagerClass())
        .in(Singleton.class);
    bind(DistributedTransactionAdmin.class)
        .to(config.getTransactionAdminClass())
        .in(Singleton.class);
    if (config.getTwoPhaseCommitTransactionManagerClass() != null) {
      bind(TwoPhaseCommitTransactionManager.class)
          .to(config.getTwoPhaseCommitTransactionManagerClass())
          .in(Singleton.class);
    }
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }
}
