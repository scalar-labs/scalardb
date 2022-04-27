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
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.common.AbstractDistributedStorage;
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
 * the basis of the specified configuration and a given operation. If there is a conflict between a
 * table mapping and a namespace mapping, it prefers the table mapping because table mappings are
 * more specific than namespace mappings.
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
  public MultiStorage(MultiStorageConfig config) {
    storages = new ArrayList<>();
    Map<String, DistributedStorage> nameStorageMap = new HashMap<>();
    config
        .getDatabaseConfigMap()
        .forEach(
            (storageName, databaseConfig) -> {
              StorageFactory factory = new StorageFactory(databaseConfig);
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
      MultiStorageConfig config,
      Map<String, DistributedStorage> tableStorageMap,
      Map<String, DistributedStorage> namespaceStorageMap,
      DistributedStorage defaultStorage) {
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
    checkArgument(mutations.size() != 0);
    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
      return;
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
