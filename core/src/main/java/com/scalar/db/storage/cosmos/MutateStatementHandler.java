package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  private static final String MUTATION_STORED_PROCEDURE = "mutate.js";

  public MutateStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  /**
   * Executes the specified {@code Mutation}
   *
   * @param mutation a {@code Mutation} to execute
   * @throws ExecutionException if the execution fails
   */
  public void handle(Mutation mutation) throws ExecutionException {
    try {
      execute(mutation);
    } catch (CosmosException e) {
      throwException(e, mutation);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          CoreError.COSMOS_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }

  abstract void execute(Mutation mutation) throws CosmosException, ExecutionException;

  protected void executeStoredProcedure(Mutation mutation, TableMetadata tableMetadata)
      throws CosmosException {
    CosmosMutation cosmosMutation = new CosmosMutation(mutation, tableMetadata);
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

  private void throwException(CosmosException exception, Mutation mutation)
      throws ExecutionException {
    int statusCode = exception.getSubStatusCode();

    if (statusCode == CosmosErrorCode.PRECONDITION_FAILED.get()) {
      throw new NoMutationException(
          CoreError.NO_MUTATION_APPLIED.buildMessage(),
          Collections.singletonList(mutation),
          exception);
    } else if (statusCode == CosmosErrorCode.RETRY_WITH.get()) {
      throw new RetriableExecutionException(
          CoreError.COSMOS_RETRY_WITH_ERROR_OCCURRED_IN_MUTATION.buildMessage(
              exception.getMessage()),
          exception);
    }

    throw new ExecutionException(
        CoreError.COSMOS_ERROR_OCCURRED_IN_MUTATION.buildMessage(exception.getMessage()),
        exception);
  }
}
