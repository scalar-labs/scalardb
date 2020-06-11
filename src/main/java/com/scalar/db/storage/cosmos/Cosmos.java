package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
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
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage implementation with Cosmos DB for {@link DistributedStorage}.
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class Cosmos implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Cosmos.class);
  private final CosmosClient client;
  private final TableMetadataHandler metadataHandler;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public Cosmos(DatabaseConfig config) {
    this.client =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword())
            .directMode()
            .buildClient();

    this.metadataHandler = new TableMetadataHandler(client);

    this.selectStatementHandler = new SelectStatementHandler(client, metadataHandler);
    this.putStatementHandler = new PutStatementHandler(client, metadataHandler);
    this.deleteStatementHandler = new DeleteStatementHandler(client, metadataHandler);

    LOGGER.info("Cosmos DB object is created properly.");

    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    setTargetToIfNot(get);

    List<Record> records = selectStatementHandler.handle(get);

    if (records.isEmpty() || records.get(0) == null) {
      return Optional.empty();
    }

    TableMetadata metadata = metadataHandler.getTableMetadata(get);
    return Optional.of(new ResultImpl(records.get(0), get, metadata));
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    setTargetToIfNot(scan);

    List<Record> records = selectStatementHandler.handle(scan);

    TableMetadata metadata = metadataHandler.getTableMetadata(scan);
    return new ScannerImpl(records, scan, metadata);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    setTargetToIfNot(put);
    checkIfPrimaryKeyExists(put);

    putStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    setTargetToIfNot(delete);
    deleteStatementHandler.handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    // TODO: BatchStatementHandler
    for (Mutation m : mutations) {
      if (m instanceof Put) {
        put((Put) m);
      } else if (m instanceof Delete) {
        delete((Delete) m);
      }
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private void setTargetToIfNot(Operation operation) {
    if (!operation.forNamespace().isPresent()) {
      operation.forNamespace(namespace.orElse(null));
    }
    if (!operation.forTable().isPresent()) {
      operation.forTable(tableName.orElse(null));
    }
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
  }

  private void checkIfPrimaryKeyExists(Put put) {
    TableMetadata metadata = metadataHandler.getTableMetadata(put);

    throwIfNotMatched(Optional.of(put.getPartitionKey()), metadata.getPartitionKeyNames());
    throwIfNotMatched(put.getClusteringKey(), metadata.getClusteringKeyNames());
  }

  private void throwIfNotMatched(Optional<Key> key, Set<String> names) {
    String message = "The primary key is not properly specified.";
    if ((!key.isPresent() && names.size() > 0)
        || (key.isPresent() && (key.get().size() != names.size()))) {
      throw new IllegalArgumentException(message);
    }
    key.ifPresent(
        k ->
            k.forEach(
                v -> {
                  if (!names.contains(v.getName())) {
                    throw new IllegalArgumentException(message);
                  }
                }));
  }
}
