package com.scalar.database.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.api.Mutation;
import com.scalar.database.exception.storage.MultiPartitionException;
import com.scalar.database.exception.storage.NoMutationException;
import com.scalar.database.exception.storage.RetriableExecutionException;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An executor for a batch statement
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class BatchHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);
  private final Session session;
  private final StatementHandlerManager handlers;

  /**
   * Constructs a {@code BatchHandler} with the specified {@link Session} and {@link
   * StatementHandlerManager}
   *
   * @param session {@code Session} to create a statement with
   * @param handlers {@code StatementHandlerManager}
   */
  public BatchHandler(Session session, StatementHandlerManager handlers) {
    this.session = checkNotNull(session);
    this.handlers = checkNotNull(handlers);
  }

  /**
   * Execute the specified list of {@link Mutation}s in batch. All the {@link Mutation}s in the list
   * must be for the same partition. Otherwise, it throws a {@link MultiPartitionException}.
   *
   * @param mutations a list of {@code Mutation}s to execute
   * @throws RetriableExecutionException if it failed, but it can be retried
   * @throws NoMutationException if at least one of conditional {@code Mutation}s failed because it
   *     didn't meet the condition
   */
  public void handle(List<? extends Mutation> mutations)
      throws RetriableExecutionException, NoMutationException {
    checkNotNull(mutations);
    if (mutations.size() < 1) {
      throw new IllegalArgumentException("please specify at least one mutation.");
    }

    try {
      ResultSet results = execute(mutations);
      // it's for conditional update. non-conditional update always return true
      if (!results.wasApplied()) {
        throw new NoMutationException("no mutation was applied.");
      }
    } catch (WriteTimeoutException e) {
      LOGGER.warn("write timeout happened during batch mutate operation.", e);
      WriteType writeType = e.getWriteType();
      if (writeType == WriteType.BATCH_LOG) {
        throw new RetriableExecutionException("logging failed in the batch.", e);
      } else if (writeType == WriteType.BATCH) {
        LOGGER.warn("logging was succeeded, but mutations in the batch partially failed.", e);
      } else {
        throw new RetriableExecutionException(
            "operation failed in the batch with type " + writeType, e);
      }
    } catch (RuntimeException e) {
      LOGGER.warn(e.getMessage(), e);
      throw new RetriableExecutionException(e.getMessage(), e);
    }
  }

  private ResultSet execute(List<? extends Mutation> mutations) {
    BatchStatement batch = new BatchStatement();
    BatchComposer composer = new BatchComposer(batch, handlers);

    boolean conditional = false;
    Mutation first = mutations.get(0);
    for (Mutation mutation : mutations) {
      if (mutation.getCondition().isPresent()) {
        conditional = true;
      }
      if (!mutation.forTable().equals(first.forTable())
          || !mutation.getPartitionKey().equals(first.getPartitionKey())) {
        throw new MultiPartitionException(
            "decided not to execute this batch "
                + "since multi-partition batch is not recommended.");
      }
      // appropriate statement handler is selected here
      mutation.accept(composer);
    }

    if (conditional) {
      setConsistencyForConditionalMutation(batch);
    }
    return session.execute(batch);
  }

  @VisibleForTesting
  void setConsistencyForConditionalMutation(BatchStatement statement) {
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM); // for learn phase
    statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL); // for paxos phase
  }
}
