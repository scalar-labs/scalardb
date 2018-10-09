package com.scalar.database.service;

import com.google.inject.Inject;
import com.scalar.database.api.Delete;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.Get;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Put;
import com.scalar.database.api.Result;
import com.scalar.database.api.Scan;
import com.scalar.database.api.Scanner;
import com.scalar.database.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class StorageService implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageService.class);
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
