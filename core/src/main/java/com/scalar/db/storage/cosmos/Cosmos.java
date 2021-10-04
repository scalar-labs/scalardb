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
import com.scalar.db.storage.common.TableMetadataManager;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.util.Utility;
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
public class Cosmos implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Cosmos.class);

  private final CosmosClient client;
  private final TableMetadataManager metadataManager;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private final BatchHandler batchHandler;
  private final OperationChecker operationChecker;
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public Cosmos(CosmosConfig config) {
    client =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword().orElse(null))
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();

    namespace = Optional.empty();
    tableName = Optional.empty();

    metadataManager = new TableMetadataManager(new CosmosAdmin(client, config), config);
    operationChecker = new OperationChecker(metadataManager);

    selectStatementHandler = new SelectStatementHandler(client, metadataManager);
    putStatementHandler = new PutStatementHandler(client, metadataManager);
    deleteStatementHandler = new DeleteStatementHandler(client, metadataManager);
    batchHandler = new BatchHandler(client, metadataManager);

    LOGGER.info("Cosmos DB object is created properly.");
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
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    Utility.setTargetToIfNot(get, namespace, tableName);
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
    Utility.setTargetToIfNot(scan, namespace, tableName);
    operationChecker.check(scan);

    List<Record> records = selectStatementHandler.handle(scan);

    TableMetadata metadata = metadataManager.getTableMetadata(scan);
    return new ScannerImpl(records, new ResultInterpreter(scan.getProjections(), metadata));
  }

  @Override
  public void put(Put put) throws ExecutionException {
    Utility.setTargetToIfNot(put, namespace, tableName);
    operationChecker.check(put);

    putStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    Utility.setTargetToIfNot(delete, namespace, tableName);
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

    Utility.setTargetToIfNot(mutations, namespace, tableName);
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
