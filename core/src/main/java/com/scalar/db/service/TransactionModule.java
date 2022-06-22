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

/** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
@Deprecated
public class TransactionModule extends AbstractModule {

  private final DatabaseConfig config;

  public TransactionModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(DistributedStorage.class).to(config.getStorageClass());
    bind(DistributedStorageAdmin.class).to(config.getStorageAdminClass());

    bind(DistributedTransactionManager.class).to(config.getTransactionManagerClass());
    bind(DistributedTransactionAdmin.class).to(config.getTransactionAdminClass());
    if (config.getTwoPhaseCommitTransactionManagerClass() != null) {
      bind(TwoPhaseCommitTransactionManager.class)
          .to(config.getTwoPhaseCommitTransactionManagerClass());
    }
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }
}
