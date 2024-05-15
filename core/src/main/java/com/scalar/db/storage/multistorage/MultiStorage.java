package com.scalar.db.storage.multistorage;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
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
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.HashMap;
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

  private final Map<String, DistributedStorage> tableStorageMap;
  private final Map<String, DistributedStorage> namespaceStorageMap;
  private final DistributedStorage defaultStorage;
  private final List<DistributedStorage> storages;

  @Inject
  public MultiStorage(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    MultiStorageConfig config = new MultiStorageConfig(databaseConfig);

    storages = new ArrayList<>();
    Map<String, DistributedStorage> nameStorageMap = new HashMap<>();
    config
        .getDatabasePropertiesMap()
        .forEach(
            (storageName, properties) -> {
              StorageFactory factory = StorageFactory.create(properties);
              DistributedStorage storage = factory.getStorage();
              nameStorageMap.put(storageName, storage);
              storages.add(storage);
            });

    tableStorageMap = new HashMap<>();
    config
        .getTableStorageMap()
        .forEach(
            (table, storageName) -> tableStorageMap.put(table, nameStorageMap.get(storageName)));

    namespaceStorageMap = new HashMap<>();
    config
        .getNamespaceStorageMap()
        .forEach(
            (table, storageName) ->
                namespaceStorageMap.put(table, nameStorageMap.get(storageName)));

    defaultStorage = nameStorageMap.get(config.getDefaultStorage());
  }

  @VisibleForTesting
  MultiStorage(
      DatabaseConfig databaseConfig,
      Map<String, DistributedStorage> tableStorageMap,
      Map<String, DistributedStorage> namespaceStorageMap,
      DistributedStorage defaultStorage) {
    super(databaseConfig);
    this.tableStorageMap = tableStorageMap;
    this.namespaceStorageMap = namespaceStorageMap;
    this.defaultStorage = defaultStorage;
    storages = null;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    return getStorage(get).get(get);
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
    String fullTaleName = operation.forFullTableName().get();
    DistributedStorage storage = tableStorageMap.get(fullTaleName);
    if (storage != null) {
      return storage;
    }
    String namespace = operation.forNamespace().get();
    storage = namespaceStorageMap.get(namespace);
    return storage != null ? storage : defaultStorage;
  }

  @Override
  public void close() {
    for (DistributedStorage storage : storages) {
      storage.close();
    }
  }
}
