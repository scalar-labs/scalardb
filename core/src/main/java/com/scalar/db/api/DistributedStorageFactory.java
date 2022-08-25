package com.scalar.db.api;

import com.scalar.db.config.DatabaseConfig;

/**
 * A factory class that creates {@link DistributedStorage} and {@link DistributedStorageAdmin}
 * instances. Each storage adapter should implement this class to instantiate its implementations of
 * {@link DistributedStorage} and {@link DistributedStorageAdmin}. The implementations are assumed
 * to be loaded by {@link java.util.ServiceLoader}.
 */
public interface DistributedStorageFactory {

  /**
   * Returns the name of the adapter. This is for the configuration "scalar.db.storage".
   *
   * @return the name of the adapter
   */
  String getName();

  /**
   * Creates an instance of {@link DistributedStorage} for the adapter.
   *
   * @param config a database config
   * @return an instance of {@link DistributedStorage} for the adapter
   */
  DistributedStorage getDistributedStorage(DatabaseConfig config);

  /**
   * Creates an instance of {@link DistributedStorageAdmin} for the adapter.
   *
   * @param config a database config
   * @return an instance of {@link DistributedStorageAdmin} for the adapter
   */
  DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config);
}
