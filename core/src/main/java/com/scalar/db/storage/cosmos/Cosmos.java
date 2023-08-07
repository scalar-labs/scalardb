package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkArgument;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.google.common.annotations.VisibleForTesting;
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
import com.scalar.db.util.ScalarDbUtils;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage implementation with Cosmos DB for {@link DistributedStorage}
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class Cosmos extends AbstractDistributedStorage {
  private static final Logger logger = LoggerFactory.getLogger(Cosmos.class);

  private final CosmosClient client;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private final BatchHandler batchHandler;
  private final OperationChecker operationChecker;

  @Inject
  public Cosmos(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    CosmosConfig config = new CosmosConfig(databaseConfig);

    client =
        new CosmosClientBuilder()
            .endpoint(config.getEndpoint())
            .key(config.getKey())
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();

    TableMetadataManager metadataManager =
        new TableMetadataManager(
            new CosmosAdmin(client, config), databaseConfig.getMetadataCacheExpirationTimeSecs());
    operationChecker = new CosmosOperationChecker(metadataManager);

    selectStatementHandler = new SelectStatementHandler(client, metadataManager);
    putStatementHandler = new PutStatementHandler(client, metadataManager);
    deleteStatementHandler = new DeleteStatementHandler(client, metadataManager);
    batchHandler = new BatchHandler(client, metadataManager);

    logger.info("Cosmos DB object is created properly");
  }

  @VisibleForTesting
  Cosmos(
      DatabaseConfig databaseConfig,
      CosmosClient client,
      SelectStatementHandler select,
      PutStatementHandler put,
      DeleteStatementHandler delete,
      BatchHandler batch,
      OperationChecker operationChecker) {
    super(databaseConfig);
    this.client = client;
    this.selectStatementHandler = select;
    this.putStatementHandler = put;
    this.deleteStatementHandler = delete;
    this.batchHandler = batch;
    this.operationChecker = operationChecker;
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    operationChecker.check(get);

    Scanner scanner = selectStatementHandler.handle(get);
    Optional<Result> ret = scanner.one();
    if (scanner.one().isPresent()) {
      throw new IllegalArgumentException("Please use scan() for non-exact match selection");
    }

    return ret;
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    operationChecker.check(scan);

    if (ScalarDbUtils.isRelational(scan)) {
      throw new UnsupportedOperationException(
          "Scanning all records with orderings or conditions is not supported in Cosmos DB");
    }

    return selectStatementHandler.handle(scan);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    operationChecker.check(put);

    putStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    delete = copyAndSetTargetToIfNot(delete);
    operationChecker.check(delete);

    deleteStatementHandler.handle(delete);
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
    operationChecker.check(mutations);
    batchHandler.handle(mutations);
  }

  @Override
  public void close() {
    client.close();
  }
}
