package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.dynamo.DynamoConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.multistorage.MultiStorageConfig;

public class StorageModule extends AbstractModule {
  private final DatabaseConfig config;

  public StorageModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(DistributedStorage.class).to(config.getStorageClass()).in(Singleton.class);
    bind(DistributedStorageAdmin.class).to(config.getAdminClass()).in(Singleton.class);
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }

  @Singleton
  @Provides
  DynamoConfig provideDynamoConfig() {
    return new DynamoConfig(config.getProperties());
  }

  @Singleton
  @Provides
  JdbcConfig provideJdbcConfig() {
    return new JdbcConfig(config.getProperties());
  }

  @Singleton
  @Provides
  MultiStorageConfig provideMultiStorageConfig() {
    return new MultiStorageConfig(config.getProperties());
  }
}
