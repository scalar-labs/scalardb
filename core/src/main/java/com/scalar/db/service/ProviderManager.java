package com.scalar.db.service;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;

/**
 * This class loads {@link DistributedStorageProvider} and {@link DistributedTransactionProvider}
 * instances with {@link ServiceLoader} and cache them.
 */
final class ProviderManager {

  // cache for DistributedStorageProvider instances
  private static final Map<String, DistributedStorageProvider> DISTRIBUTED_STORAGE_PROVIDERS;

  // cache for DistributedTransactionProvider instances
  private static final Map<String, DistributedTransactionProvider>
      DISTRIBUTED_TRANSACTION_PROVIDERS;

  private ProviderManager() {}

  static {
    // load DistributedStorageProvider with ServiceLoader
    ImmutableMap.Builder<String, DistributedStorageProvider> distributedStorageProvidersBuilder =
        ImmutableMap.builder();
    ServiceLoader<DistributedStorageProvider> distributedStorageProviderServiceLoader =
        ServiceLoader.load(DistributedStorageProvider.class);
    for (DistributedStorageProvider distributedStorageProvider :
        distributedStorageProviderServiceLoader) {
      distributedStorageProvidersBuilder.put(
          distributedStorageProvider.getName().toLowerCase(Locale.ROOT),
          distributedStorageProvider);
    }
    DISTRIBUTED_STORAGE_PROVIDERS = distributedStorageProvidersBuilder.build();

    // load DistributedTransactionProvider with ServiceLoader
    ImmutableMap.Builder<String, DistributedTransactionProvider>
        distributedTransactionProvidersBuilder = ImmutableMap.builder();
    ServiceLoader<DistributedTransactionProvider> distributedTransactionProviderServiceLoader =
        ServiceLoader.load(DistributedTransactionProvider.class);
    for (DistributedTransactionProvider distributedTransactionProvider :
        distributedTransactionProviderServiceLoader) {
      distributedTransactionProvidersBuilder.put(
          distributedTransactionProvider.getName().toLowerCase(Locale.ROOT),
          distributedTransactionProvider);
    }
    DISTRIBUTED_TRANSACTION_PROVIDERS = distributedTransactionProvidersBuilder.build();
  }

  /**
   * Returns an instance of {@link DistributedStorage}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedStorage}
   */
  public static DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return getDistributedStorageProvider(config.getStorage()).createDistributedStorage(config);
  }

  /**
   * Returns an instance of {@link DistributedStorageAdmin}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedStorageAdmin}
   */
  public static DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return getDistributedStorageProvider(config.getStorage()).createDistributedStorageAdmin(config);
  }

  private static DistributedStorageProvider getDistributedStorageProvider(String name) {
    String lowerCaseName = name.toLowerCase(Locale.ROOT);
    if (!DISTRIBUTED_STORAGE_PROVIDERS.containsKey(lowerCaseName)) {
      throw new IllegalArgumentException(CoreError.STORAGE_NOT_FOUND.buildMessage(name));
    }
    return DISTRIBUTED_STORAGE_PROVIDERS.get(lowerCaseName);
  }

  /**
   * Returns an instance of {@link DistributedTransactionManager}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionManager}
   */
  public static DistributedTransactionManager createDistributedTransactionManager(
      DatabaseConfig config) {
    return getDistributedTransactionProvider(config.getTransactionManager())
        .createDistributedTransactionManager(config);
  }

  /**
   * Returns an instance of {@link DistributedTransactionAdmin}.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionAdmin}
   */
  public static DistributedTransactionAdmin createDistributedTransactionAdmin(
      DatabaseConfig config) {
    return getDistributedTransactionProvider(config.getTransactionManager())
        .createDistributedTransactionAdmin(config);
  }

  /**
   * Returns an instance of {@link TwoPhaseCommitTransactionManager}.
   *
   * @param config a database config
   * @return an instance of {@link TwoPhaseCommitTransactionManager}. If the transaction manager
   *     does not support the two-phase commit interface, returns {@code null}.
   */
  @Nullable
  public static TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return getDistributedTransactionProvider(config.getTransactionManager())
        .createTwoPhaseCommitTransactionManager(config);
  }

  private static DistributedTransactionProvider getDistributedTransactionProvider(String name) {
    String lowerCaseName = name.toLowerCase(Locale.ROOT);
    if (!DISTRIBUTED_TRANSACTION_PROVIDERS.containsKey(lowerCaseName)) {
      throw new IllegalArgumentException(
          CoreError.TRANSACTION_MANAGER_NOT_FOUND.buildMessage(name));
    }
    return DISTRIBUTED_TRANSACTION_PROVIDERS.get(lowerCaseName);
  }
}
