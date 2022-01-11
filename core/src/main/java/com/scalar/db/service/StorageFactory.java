package com.scalar.db.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

/** A factory class to instantiate {@link DistributedStorage} and {@link DistributedStorageAdmin} */
public class StorageFactory {
  private final Injector injector;

  public StorageFactory(DatabaseConfig config) {
    injector = Guice.createInjector(new StorageModule(config));
  }

  /**
   * Returns a {@link DistributedStorage} instance
   *
   * @return a {@link DistributedStorage} instance
   */
  public DistributedStorage getStorage() {
    return injector.getInstance(DistributedStorage.class);
  }

  /**
   * Returns a {@link DistributedStorageAdmin} instance
   *
   * @return a {@link DistributedStorageAdmin} instance
   */
  public DistributedStorageAdmin getAdmin() {
    return injector.getInstance(DistributedStorageAdmin.class);
  }
}
