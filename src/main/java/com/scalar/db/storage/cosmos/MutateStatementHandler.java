package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutateStatementHandler.class);
  private final String MUTATION_STORED_PROCEDURE = "mutate.js";

  public MutateStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  @Nonnull
  public List<Record> handle(Operation operation) throws ExecutionException {
    try {
      List<Record> results = execute(operation);

      return results;
    } catch (CosmosException e) {
      throwException(e);
    }

    return Collections.emptyList();
  }

  protected void executeStoredProcedure(Mutation mutation) throws CosmosException {
    CosmosMutation cosmosMutation = getCosmosMutation(mutation);
    List<Object> args = new ArrayList<>();
    args.add(1);
    args.add(cosmosMutation.getMutationType().ordinal());
    args.add(cosmosMutation.makeRecord());
    args.add(cosmosMutation.makeConditionalQuery());

    getContainer(mutation)
        .getScripts()
        .getStoredProcedure(MUTATION_STORED_PROCEDURE)
        .execute(args, cosmosMutation.getStoredProcedureOptions());
  }

  private void throwException(CosmosException exception) throws ExecutionException {
    LOGGER.error(exception.getMessage());
    int statusCode = exception.getSubStatusCode();

    if (statusCode == CosmosErrorCode.PRECONDITION_FAILED.get()) {
      throw new NoMutationException("no mutation was applied.");
    } else if (statusCode == CosmosErrorCode.RETRY_WITH.get()) {
      throw new RetriableExecutionException(exception.getMessage(), exception);
    }

    throw new ExecutionException(exception.getMessage(), exception);
  }

  private CosmosMutation getCosmosMutation(Mutation mutation) {
    TableMetadata metadata = metadataManager.getTableMetadata(mutation);

    return new CosmosMutation(mutation, metadata);
  }
}
