package com.scalar.database.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
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
 * @author Hiroyuki Yamada, Yuji Ito
 */
@ThreadSafe
public class BatchHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);
  private final CqlSession session;
  private final StatementHandlerManager handlers;

  /**
   * Constructs a {@code BatchHandler} with the specified {@link CqlSession} and {@link
   * StatementHandlerManager}
   *
   * @param session {@code CqlSession} to create a statement with
   * @param handlers {@code StatementHandlerManager}
   */
  public BatchHandler(CqlSession session, StatementHandlerManager handlers) {
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
    BatchStatementBuilder builder = new BatchStatementBuilder(BatchType.LOGGED);
    BatchComposer composer = new BatchComposer(builder, handlers);

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
      setConsistencyForConditionalMutation(builder);
    }
    return session.execute(builder.build());
  }

  @VisibleForTesting
  void setConsistencyForConditionalMutation(BatchStatementBuilder builder) {
    builder.setConsistencyLevel(ConsistencyLevel.QUORUM); // for learn phase
    builder.setSerialConsistencyLevel(ConsistencyLevel.SERIAL); // for paxos phase
  }
}
