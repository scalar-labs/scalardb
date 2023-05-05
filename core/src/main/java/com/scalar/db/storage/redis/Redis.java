package com.scalar.db.storage.redis;

import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.AbstractDistributedStorage;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cassandra.Cassandra;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A storage implementation with Redis for {@link DistributedStorage}. */
@ThreadSafe
public class Redis extends AbstractDistributedStorage {
  private static final Logger logger = LoggerFactory.getLogger(Cassandra.class);
  private final TableMetadataManager metadataManager;
  private final OperationChecker operationChecker;

  @Inject
  public Redis(DatabaseConfig config) {
    super(config);

    metadataManager =
        new TableMetadataManager(
            new RedisAdmin(config), config.getMetadataCacheExpirationTimeSecs());
    operationChecker = new OperationChecker(metadataManager);
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    return Optional.empty();
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    return null;
  }

  @Override
  public void put(Put put) throws ExecutionException {}

  @Override
  public void put(List<Put> puts) throws ExecutionException {}

  @Override
  public void delete(Delete delete) throws ExecutionException {}

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {}

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {}

  @Override
  public void close() {}
}
