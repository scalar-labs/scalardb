package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/** @deprecated As of release 3.5.0. Will be removed in release 5.0.0 */
@Deprecated
@ThreadSafe
public class StorageService implements DistributedStorage {
  private final DistributedStorage storage;

  @Inject
  public StorageService(DistributedStorage storage) {
    this.storage = storage;
  }

  @Override
  public void with(String namespace, String tableName) {
    storage.with(namespace, tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    storage.withNamespace(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return storage.getNamespace();
  }

  @Override
  public void withTable(String tableName) {
    storage.withTable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return storage.getTable();
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    return storage.get(get);
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    return storage.scan(scan);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    storage.put(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    storage.put(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    storage.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    storage.delete(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    storage.mutate(mutations);
  }

  @Override
  public void close() {
    storage.close();
  }
}
