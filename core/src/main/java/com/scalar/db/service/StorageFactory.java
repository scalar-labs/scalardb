package com.scalar.db.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/** A factory class to instantiate {@link DistributedStorage} and {@link DistributedStorageAdmin} */
public class StorageFactory {
  private final Injector injector;

  /**
   * @param config a database config
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #create(Properties)}, {@link #create(Path)}, {@link #create(File)}, or {@link
   *     #create(String)} instead
   */
  @Deprecated
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
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #getStorageAdmin()} instead
   */
  @Deprecated
  public DistributedStorageAdmin getAdmin() {
    return injector.getInstance(DistributedStorageAdmin.class);
  }

  /**
   * Returns a {@link DistributedStorageAdmin} instance
   *
   * @return a {@link DistributedStorageAdmin} instance
   */
  public DistributedStorageAdmin getStorageAdmin() {
    return injector.getInstance(DistributedStorageAdmin.class);
  }

  public static StorageFactory create(Properties properties) {
    return new StorageFactory(new DatabaseConfig(properties));
  }

  public static StorageFactory create(Path propertiesPath) throws IOException {
    return new StorageFactory(new DatabaseConfig(propertiesPath));
  }

  public static StorageFactory create(File propertiesFile) throws IOException {
    return new StorageFactory(new DatabaseConfig(propertiesFile));
  }

  /**
   * Returns a StorageFactory instance.
   *
   * @param propertiesFilePath a properties file path
   * @return a StorageFactory instance
   * @throws IOException if IO error occurs
   */
  public static StorageFactory create(String propertiesFilePath) throws IOException {
    return create(Paths.get(propertiesFilePath));
  }
}
