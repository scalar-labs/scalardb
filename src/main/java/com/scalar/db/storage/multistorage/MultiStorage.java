package com.scalar.db.storage.multistorage;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
import com.scalar.db.service.StorageModule;
import com.scalar.db.storage.common.util.Utility;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * A storage implementation with multi-storage for {@link DistributedStorage}.
 *
 * <p>This storage implementation holds multiple storage instances and have mapping a table name to
 * a proper storage instance. When a operation executes, it chooses a proper storage instance from
 * the specified table name by using the table-storage mapping and uses it.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class MultiStorage implements DistributedStorage {

  private final Map<String, DistributedStorage> storageMap;
  private final DistributedStorage defaultStorage;

  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public MultiStorage(MultiStorageConfig config) {
    Map<String, DistributedStorage> nameStorageMap = new HashMap<>();
    config
        .getDatabaseConfigMap()
        .forEach(
            (storage, databaseConfig) -> {
              // Instantiate storages with Guice
              Injector injector = Guice.createInjector(new StorageModule(databaseConfig));
              nameStorageMap.put(storage, injector.getInstance(DistributedStorage.class));
            });

    Builder<String, DistributedStorage> builder = ImmutableMap.builder();
    config
        .getTableStorageMap()
        .forEach((table, storage) -> builder.put(table, nameStorageMap.get(storage)));
    storageMap = builder.build();

    defaultStorage = nameStorageMap.get(config.getDefaultStorage());

    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    return getStorage(get).get(get);
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    return getStorage(scan).scan(scan);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    getStorage(put).put(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
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

    getStorage(mutations.get(0)).mutate(mutations);
  }

  private DistributedStorage getStorage(Operation operation) {
    Utility.setTargetToIfNot(operation, Optional.empty(), namespace, tableName);
    String fullTaleName = operation.forFullTableName().get();
    DistributedStorage storage = storageMap.get(fullTaleName);
    if (storage == null) {
      return defaultStorage;
    }
    return storage;
  }

  @Override
  public void close() {
    for (DistributedStorage storage : storageMap.values()) {
      storage.close();
    }
  }
}
