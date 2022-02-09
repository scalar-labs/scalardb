package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.util.TableMetadataManager;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler for a batch
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class BatchHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);
  private static final String MUTATION_STORED_PROCEDURE = "mutate.js";
  private final CosmosClient client;
  private final TableMetadataManager metadataManager;

  /**
   * Constructs a {@code BatchHandler} with the specified {@link CosmosClient} and {@link
   * TableMetadataManager}
   *
   * @param client {@code CosmosClient} to create a statement with
   * @param metadataManager {@code TableMetadataManager}
   */
  public BatchHandler(CosmosClient client, TableMetadataManager metadataManager) {
    this.client = client;
    this.metadataManager = metadataManager;
  }

  /**
   * Execute the specified list of {@link Mutation}s in batch. All the {@link Mutation}s in the list
   * must be for the same partition.
   *
   * @param mutations a list of {@code Mutation}s to execute
   * @throws NoMutationException if at least one of conditional {@code Mutation}s failed because it
   *     didn't meet the condition
   */
  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutations.get(0));
    try {
      executeStoredProcedure(mutations, tableMetadata);
    } catch (CosmosException e) {
      throwException(e);
    }
  }

  private void executeStoredProcedure(
      List<? extends Mutation> mutations, TableMetadata tableMetadata) throws CosmosException {
    List<Integer> types = new ArrayList<>();
    List<Record> records = new ArrayList<>();
    List<String> queries = new ArrayList<>();

    mutations.forEach(
        mutation -> {
          CosmosMutation cosmosMutation = new CosmosMutation(mutation, tableMetadata);
          types.add(cosmosMutation.getMutationType().ordinal());
          records.add(cosmosMutation.makeRecord());
          queries.add(cosmosMutation.makeConditionalQuery());
        });
    List<Object> args = new ArrayList<>();
    args.add(mutations.size());
    args.addAll(types);
    args.addAll(records);
    args.addAll(queries);

    CosmosMutation cosmosMutation = new CosmosMutation(mutations.get(0), tableMetadata);
    client
        .getDatabase(mutations.get(0).forNamespace().get())
        .getContainer(mutations.get(0).forTable().get())
        .getScripts()
        .getStoredProcedure(MUTATION_STORED_PROCEDURE)
        .execute(args, cosmosMutation.getStoredProcedureOptions());
  }

  private void throwException(CosmosException exception) throws ExecutionException {
    LOGGER.error(exception.getMessage(), exception);
    int statusCode = exception.getSubStatusCode();

    if (statusCode == CosmosErrorCode.PRECONDITION_FAILED.get()) {
      throw new NoMutationException("no mutation was applied.");
    } else if (statusCode == CosmosErrorCode.RETRY_WITH.get()) {
      throw new RetriableExecutionException(exception.getMessage(), exception);
    }

    throw new ExecutionException(exception.getMessage(), exception);
  }
}
