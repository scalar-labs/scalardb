package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.db.transaction.jdbc.JdbcTransactionManager;

public class TransactionModule extends AbstractModule {
  private final DatabaseConfig config;

  public TransactionModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(DistributedStorage.class).to(config.getStorageClass()).in(Singleton.class);

    if (config.getStorageClass() == JdbcDatabase.class) {
      JdbcDatabaseConfig jdbcDatabaseConfig = provideJdbcDatabaseConfig();
      if (jdbcDatabaseConfig
          .getTransactionManagerType()
          .equals(JdbcDatabaseConfig.TRANSACTION_MANAGER_TYPE_JDBC)) {
        bind(DistributedTransactionManager.class)
            .to(JdbcTransactionManager.class)
            .in(Singleton.class);
        return;
      }
    }

    bind(DistributedTransactionManager.class).to(ConsensusCommitManager.class).in(Singleton.class);
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }

  @Singleton
  @Provides
  JdbcDatabaseConfig provideJdbcDatabaseConfig() {
    return new JdbcDatabaseConfig(config.getProperties());
  }
}
