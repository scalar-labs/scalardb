package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.InvalidUsageException;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.exception.storage.NoMutationException;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler for a batch statement
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class BatchStatementHandler extends MutateStatementHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(BatchStatementHandler.class);

  /**
   * Constructs a {@code BatchHandler} with the specified {@link CosmosClient} and {@link
   * TableMetadataManager}
   *
   * @param client {@code CosmosClient} to create a statement with
   * @param metadataManager {@code TableMetadataManager}
   */
  public BatchStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  /**
   * Execute the specified list of {@link Mutation}s in batch. All the {@link Mutation}s in the list
   * must be for the same partition. Otherwise, it throws an {@link MultiPartitionException}.
   *
   * @param mutations a list of {@code Mutation}s to execute
   * @throws MultiPartitionException if the mutations are for multiple partitions
   * @throws NoMutationException if at least one of conditional {@code Mutation}s failed because it
   *     didn't meet the condition
   */
  public void handle(List<? extends Mutation> mutations) throws InvalidUsageException, ExecutionException {
    checkNotNull(mutations);
    if (mutations.size() < 1) {
      throw new IllegalArgumentException("please specify at least one mutation.");
    }

    Mutation first = mutations.get(0);
    for (Mutation mutation : mutations) {
      if (!mutation.forTable().equals(first.forTable())
          || !mutation.getPartitionKey().equals(first.getPartitionKey())) {
        throw new MultiPartitionException(
            "decided not to execute this batch "
                + "since multi-partition batch is not recommended.");
      }
    }

    try {
      executeStoredProcedure(mutations);
    } catch (CosmosException e) {
      throwException(e);
    }
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosException {
    return Collections.emptyList();
  };
}
