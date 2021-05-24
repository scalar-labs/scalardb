package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.storage.multistorage.MultiStorageConfig;

public class AdminModule extends AbstractModule {
  private final DatabaseConfig config;

  public AdminModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(DistributedStorageAdmin.class).to(config.getAdminClass()).in(Singleton.class);
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

  @Singleton
  @Provides
  MultiStorageConfig provideMultiStorageConfig() {
    return new MultiStorageConfig(config.getProperties());
  }
}
