package com.scalar.db.storage.multistorage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.AbstractDistributedStorage;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A storage implementation with multi-storage for {@link DistributedStorage}.
 *
 * <p>This storage implementation holds multiple storage instances. It chooses a storage instance on
 * the basis of the specified configuration and a given operation.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class MultiStorage extends AbstractDistributedStorage {

  /** @deprecated Will be removed in 4.0.0. */
  @Deprecated private final Map<String, DistributedStorage> tableStorageMap;

  private final Map<String, String> namespaceStorageNameMap;
  private final String defaultStorageName;
  private final Map<String, DistributedStorage> nameStorageMap;

  @Inject
  public MultiStorage(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    MultiStorageConfig config = new MultiStorageConfig(databaseConfig);

    nameStorageMap =
        config.getDatabasePropertiesMap().entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey, e -> StorageFactory.create(e.getValue()).getStorage()));

    tableStorageMap =
        config.getTableStorageMap().entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey, e -> nameStorageMap.get(e.getValue())));

    namespaceStorageNameMap = ImmutableMap.copyOf(config.getNamespaceStorageMap());

    defaultStorageName = config.getDefaultStorage();
  }

  @VisibleForTesting
  MultiStorage(
      DatabaseConfig databaseConfig,
      Map<String, DistributedStorage> nameStorageMap,
      Map<String, DistributedStorage> tableStorageMap,
      Map<String, String> namespaceStorageNameMap,
      String defaultStorageName) {
    super(databaseConfig);
    this.nameStorageMap = ImmutableMap.copyOf(nameStorageMap);
    this.tableStorageMap = ImmutableMap.copyOf(tableStorageMap);
    this.namespaceStorageNameMap = ImmutableMap.copyOf(namespaceStorageNameMap);
    this.defaultStorageName = defaultStorageName;
  }

  /**
   * Returns a map of underlying storages keyed by the user-defined storage name (configured via
   * {@code scalar.db.multi_storage.storages}).
   *
   * @return an unmodifiable map of underlying storages keyed by storage name
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Map<String, DistributedStorage> getNameStorageMap() {
    return nameStorageMap;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    return getStorage(get).get(get);
  }

  /**
   * Calls {@link DistributedStorage#multiGet(List)} once per storage that the Gets are routed to.
   */
  @Override
  public List<Optional<Result>> multiGet(List<Get> gets) throws ExecutionException {
    Map<DistributedStorage, List<Integer>> byStorage = new LinkedHashMap<>();
    List<Get> targeted = new ArrayList<>(gets.size());
    for (int i = 0; i < gets.size(); i++) {
      Get get = copyAndSetTargetToIfNot(gets.get(i));
      targeted.add(get);
      byStorage.computeIfAbsent(getStorage(get), s -> new ArrayList<>()).add(i);
    }
    @SuppressWarnings("unchecked")
    Optional<Result>[] results = new Optional[gets.size()];
    for (Map.Entry<DistributedStorage, List<Integer>> e : byStorage.entrySet()) {
      List<Get> sub = new ArrayList<>(e.getValue().size());
      for (int i : e.getValue()) {
        sub.add(targeted.get(i));
      }
      List<Optional<Result>> subResults = e.getKey().multiGet(sub);
      checkState(
          subResults.size() == sub.size(),
          "The storage returned %s results for %s Gets",
          subResults.size(),
          sub.size());
      for (int k = 0; k < sub.size(); k++) {
        results[e.getValue().get(k)] = subResults.get(k);
      }
    }
    return Arrays.asList(results);
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    return getStorage(scan).scan(scan);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    getStorage(put).put(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    delete = copyAndSetTargetToIfNot(delete);
    getStorage(delete).delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
        return;
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
        return;
      }
    }

    mutations = copyAndSetTargetToIfNot(mutations);
    getStorage(mutations.get(0)).mutate(mutations);
  }

  private DistributedStorage getStorage(Operation operation) {
    assert operation.forFullTableName().isPresent() && operation.forNamespace().isPresent();

    String fullTaleName = operation.forFullTableName().get();
    DistributedStorage storage = tableStorageMap.get(fullTaleName);
    if (storage != null) {
      return storage;
    }
    String namespace = operation.forNamespace().get();
    String storageName = namespaceStorageNameMap.getOrDefault(namespace, defaultStorageName);
    return nameStorageMap.get(storageName);
  }

  @Override
  public void close() {
    for (DistributedStorage storage : nameStorageMap.values()) {
      storage.close();
    }
  }
}
