package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class StorageModule extends AbstractModule {

  private final DatabaseConfig config;

  public StorageModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(DistributedStorage.class).to(config.getStorageClass()).in(Singleton.class);
    bind(DistributedStorageAdmin.class).to(config.getStorageAdminClass()).in(Singleton.class);
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }
}
