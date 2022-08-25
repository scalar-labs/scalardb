package com.scalar.db.service;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionFactory;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * This class loads {@link DistributedStorageFactory} and {@link DistributedTransactionFactory} with
 * {@link ServiceLoader}.
 */
final class FactoryManager {
  private static final Map<String, DistributedStorageFactory> DISTRIBUTED_STORAGE_FACTORIES;
  private static final Map<String, DistributedTransactionFactory> DISTRIBUTED_TRANSACTION_FACTORIES;

  private FactoryManager() {}

  static {
    // load DistributedStorageFactory with ServiceLoader
    ImmutableMap.Builder<String, DistributedStorageFactory> distributedStorageFactoriesBuilder =
        ImmutableMap.builder();
    ServiceLoader<DistributedStorageFactory> distributedStorageFactoryServiceLoader =
        ServiceLoader.load(DistributedStorageFactory.class);
    for (DistributedStorageFactory distributedStorageFactory :
        distributedStorageFactoryServiceLoader) {
      distributedStorageFactoriesBuilder.put(
          distributedStorageFactory.getName(), distributedStorageFactory);
    }
    DISTRIBUTED_STORAGE_FACTORIES = distributedStorageFactoriesBuilder.build();

    // load DistributedStorageFactory with ServiceLoader
    ImmutableMap.Builder<String, DistributedTransactionFactory>
        distributedTransactionFactoriesBuilder = ImmutableMap.builder();
    ServiceLoader<DistributedTransactionFactory> distributedTransactionFactoryServiceLoader =
        ServiceLoader.load(DistributedTransactionFactory.class);
    for (DistributedTransactionFactory distributedTransactionFactory :
        distributedTransactionFactoryServiceLoader) {
      distributedTransactionFactoriesBuilder.put(
          distributedTransactionFactory.getName(), distributedTransactionFactory);
    }
    DISTRIBUTED_TRANSACTION_FACTORIES = distributedTransactionFactoriesBuilder.build();
  }

  /**
   * Returns an instance of {@link DistributedStorage}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedStorage}
   */
  public static DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return getDistributedStorageFactory(config.getStorage()).getDistributedStorage(config);
  }

  /**
   * Returns an instance of {@link DistributedStorageAdmin}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedStorageAdmin}
   */
  public static DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return getDistributedStorageFactory(config.getStorage()).getDistributedStorageAdmin(config);
  }

  private static DistributedStorageFactory getDistributedStorageFactory(String name) {
    if (!DISTRIBUTED_STORAGE_FACTORIES.containsKey(name)) {
      throw new IllegalArgumentException("storage '" + name + "' is not found");
    }
    return DISTRIBUTED_STORAGE_FACTORIES.get(name);
  }

  /**
   * Returns an instance of {@link DistributedTransactionManager}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionManager}
   */
  public static DistributedTransactionManager getDistributedTransactionManager(
      DatabaseConfig config) {
    return getDistributedTransactionFactory(config.getTransactionManager())
        .getDistributedTransactionManager(config);
  }

  /**
   * Returns an instance of {@link DistributedTransactionAdmin}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionAdmin}
   */
  public static DistributedTransactionAdmin getDistributedTransactionAdmin(DatabaseConfig config) {
    return getDistributedTransactionFactory(config.getTransactionManager())
        .getDistributedTransactionAdmin(config);
  }

  /**
   * Returns an instance of {@link TwoPhaseCommitTransactionManager}.
   *
   * @param config a database config
   * @return an instance of {@link TwoPhaseCommitTransactionManager}
   */
  public static TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return getDistributedTransactionFactory(config.getTransactionManager())
        .getTwoPhaseCommitTransactionManager(config);
  }

  private static DistributedTransactionFactory getDistributedTransactionFactory(String name) {
    if (!DISTRIBUTED_TRANSACTION_FACTORIES.containsKey(name)) {
      throw new IllegalArgumentException("transaction manager '" + name + "' is not found");
    }
    return DISTRIBUTED_TRANSACTION_FACTORIES.get(name);
  }
}
