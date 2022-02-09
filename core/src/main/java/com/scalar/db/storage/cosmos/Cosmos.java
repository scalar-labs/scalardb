package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkArgument;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.common.AbstractDistributedStorage;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.util.TableMetadataManager;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(Cosmos.class);

  private final CosmosClient client;
  private final TableMetadataManager metadataManager;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private final BatchHandler batchHandler;
  private final OperationChecker operationChecker;

  @Inject
  public Cosmos(CosmosConfig config) {
    client =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword().orElse(null))
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();

    metadataManager =
        new TableMetadataManager(
            new CosmosAdmin(client, config), config.getTableMetadataCacheExpirationTimeSecs());
    operationChecker = new OperationChecker(metadataManager);

    selectStatementHandler = new SelectStatementHandler(client, metadataManager);
    putStatementHandler = new PutStatementHandler(client, metadataManager);
    deleteStatementHandler = new DeleteStatementHandler(client, metadataManager);
    batchHandler = new BatchHandler(client, metadataManager);

    LOGGER.info("Cosmos DB object is created properly.");
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    operationChecker.check(get);

    List<Record> records = selectStatementHandler.handle(get);
    if (records.size() > 1) {
      throw new IllegalArgumentException("please use scan() for non-exact match selection");
    }
    if (records.isEmpty() || records.get(0) == null) {
      return Optional.empty();
    }

    TableMetadata metadata = metadataManager.getTableMetadata(get);
    return Optional.of(
        new ResultInterpreter(get.getProjections(), metadata).interpret(records.get(0)));
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    operationChecker.check(scan);

    List<Record> records = selectStatementHandler.handle(scan);

    TableMetadata metadata = metadataManager.getTableMetadata(scan);
    return new ScannerImpl(records, new ResultInterpreter(scan.getProjections(), metadata));
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
    for (Mutation mutation : mutations) {
      operationChecker.check(mutation);
    }
    batchHandler.handle(mutations);
  }

  @Override
  public void close() {
    client.close();
  }
}
