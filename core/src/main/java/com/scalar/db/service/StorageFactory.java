package com.scalar.db.service;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

/** A factory class to instantiate {@link DistributedStorage} and {@link DistributedStorageAdmin} */
public class StorageFactory {
  private final DatabaseConfig config;

  /**
   * @param config a database config
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #create(Properties)}, {@link #create(Path)}, {@link #create(File)}, or {@link
   *     #create(String)} instead
   */
  @Deprecated
  public StorageFactory(DatabaseConfig config) {
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Returns a {@link DistributedStorage} instance
   *
   * @return a {@link DistributedStorage} instance
   */
  public DistributedStorage getStorage() {
    return ProviderManager.createDistributedStorage(config);
  }

  /**
   * Returns a {@link DistributedStorageAdmin} instance
   *
   * @return a {@link DistributedStorageAdmin} instance
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #getStorageAdmin()} instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public DistributedStorageAdmin getAdmin() {
    return getStorageAdmin();
  }

  /**
   * Returns a {@link DistributedStorageAdmin} instance
   *
   * @return a {@link DistributedStorageAdmin} instance
   */
  public DistributedStorageAdmin getStorageAdmin() {
    return ProviderManager.createDistributedStorageAdmin(config);
  }

  /**
   * Returns a StorageFactory instance.
   *
   * @param properties properties to use for configuration
   * @return a StorageFactory instance
   */
  public static StorageFactory create(Properties properties) {
    return new StorageFactory(new DatabaseConfig(properties));
  }

  /**
   * Returns a StorageFactory instance.
   *
   * @param propertiesPath a properties path
   * @return a StorageFactory instance
   * @throws IOException if IO error occurs
   */
  public static StorageFactory create(Path propertiesPath) throws IOException {
    return new StorageFactory(new DatabaseConfig(propertiesPath));
  }

  /**
   * Returns a StorageFactory instance.
   *
   * @param propertiesFile a properties file
   * @return a StorageFactory instance
   * @throws IOException if IO error occurs
   */
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
