package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.google.common.base.Joiner;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A handler class for statements */
@ThreadSafe
public abstract class StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatementHandler.class);
  protected final CosmosClient client;
  protected final TableMetadataManager metadataManager;

  /**
   * Constructs a {@code StatementHandler} with the specified {@link CosmosClient}
   *
   * @param client {@code CosmosClient}
   * @param metadataManager {@code TableMetadataManager}
   */
  protected StatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
  }

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation an {@code Operation} to execute
   * @return a {@code ResultSet}
   * @throws ExecutionException if the execution failed
   */
  @Nonnull
  public List<Record> handle(Operation operation) throws ExecutionException {
    try {
      List<Record> results = execute(operation);

      return results;
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  protected abstract List<Record> execute(Operation operation)
      throws CosmosException, NoMutationException;

  public static void checkArgument(Operation actual, Class<? extends Operation>... expected) {
    for (Class<? extends Operation> e : expected) {
      if (e.isInstance(actual)) {
        return;
      }
    }
    throw new IllegalArgumentException(
        Joiner.on(" ")
            .join(
                new String[] {
                  actual.getClass().toString(), "is passed where something like",
                  expected[0].toString(), "is expected."
                }));
  }

  @Nonnull
  protected CosmosContainer getContainer(Operation operation) {
    return client
        .getDatabase(operation.forNamespace().get())
        .getContainer(operation.forTable().get());
  }

  @Nonnull
  protected String getConcatenatedPartitionKey(Operation operation) {
    Map<String, Value> keyMap = new HashMap<>();
    operation
        .getPartitionKey()
        .get()
        .forEach(
            v -> {
              keyMap.put(v.getName(), v);
            });

    TableMetadata metadata = metadataManager.getTableMetadata(operation);
    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata
        .getPartitionKeyNames()
        .forEach(
            name -> {
              if (keyMap.containsKey(name)) {
                keyMap.get(name).accept(visitor);
              } else {
                throw new IllegalArgumentException("The partition key is not properly specified.");
              }
            });

    return visitor.build();
  }

  @Nonnull
  protected String getId(Operation operation) {
    Map<String, Value> keyMap = new HashMap<>();
    operation
        .getPartitionKey()
        .get()
        .forEach(
            v -> {
              keyMap.put(v.getName(), v);
            });
    operation
        .getClusteringKey()
        .ifPresent(
            k -> {
              k.get()
                  .forEach(
                      v -> {
                        keyMap.put(v.getName(), v);
                      });
            });

    TableMetadata metadata = metadataManager.getTableMetadata(operation);
    ConcatenationVisitor visitor = new ConcatenationVisitor();
    List<String> keyNames = metadata.getKeyNames();
    keyNames.forEach(
        name -> {
          if (keyMap.containsKey(name)) {
            keyMap.get(name).accept(visitor);
          } else {
            throw new IllegalArgumentException("The primary key is not properly specified.");
          }
        });

    return visitor.build();
  }
}
