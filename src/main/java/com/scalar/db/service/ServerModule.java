package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.server.DistributedStorageAdminService;
import com.scalar.db.server.DistributedStorageService;

public class ServerModule extends AbstractModule {
  private final Injector storageInjector;

  public ServerModule(DatabaseConfig config) {
    this.storageInjector = Guice.createInjector(new StorageModule(config));
  }

  @Provides
  @Singleton
  DistributedStorageService provideDistributedStorageService() {
    return storageInjector.getInstance(DistributedStorageService.class);
  }

  @Provides
  @Singleton
  DistributedStorageAdminService provideDistributedStorageAdminService() {
    return storageInjector.getInstance(DistributedStorageAdminService.class);
  }
}
