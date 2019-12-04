package com.scalar.database.storage.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.storage.NoMutationException;
import com.scalar.database.exception.storage.ReadRepairableExecutionException;
import com.scalar.database.exception.storage.RetriableExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutateStatementHandler.class);

  public MutateStatementHandler(CqlSession session) {
    super(session);
  }

  /**
   * Executes the specified {@link Mutation} {@link Operation}
   *
   * @param operation {@link Mutation} operation
   * @return a {@code ResultSet}
   * @throws RetriableExecutionException if the execution failed, but it can be retriable
   * @throws ReadRepairableExecutionException if the execution partially failed, which can be
   *     repaired by a following read
   */
  @Override
  @Nonnull
  public ResultSet handle(Operation operation) throws ExecutionException {
    try {
      ResultSet results = handleInternal(operation);

      Mutation mutation = (Mutation) operation;
      if (mutation.getCondition().isPresent() && !results.one().getBoolean(0)) {
        throw new NoMutationException("no mutation was applied.");
      }
      return results;

    } catch (WriteTimeoutException e) {
      LOGGER.warn("write timeout happened during mutate operation.", e);
      if (e.getWriteType() == WriteType.CAS) {
        // retry needs to be done if applications need to do the operation exactly
        throw new RetriableExecutionException("paxos phase in CAS operation failed.", e);
      } else if (e.getWriteType() == WriteType.SIMPLE) {
        Mutation mutation = (Mutation) operation;
        if (mutation.getCondition().isPresent()) {
          // learn phase needs to be repaired (by re-reading)
          throw new ReadRepairableExecutionException("learn phase in CAS operation failed.", e);
        } else {
          // retry needs to be done if applications need to do the operation exactly
          throw new RetriableExecutionException("simple write operation failed.", e);
        }
      } else {
        throw new ExecutionException("something wrong because it is neither CAS nor SIMPLE", e);
      }
    } catch (RuntimeException e) {
      LOGGER.warn(e.getMessage(), e);
      throw new RetriableExecutionException(e.getMessage(), e);
    }
  }

  @Override
  protected void overwriteConsistency(BoundStatementBuilder builder, Operation operation) {
    ((Mutation) operation)
        .getCondition()
        .ifPresent(
            c -> {
              builder.setConsistencyLevel(ConsistencyLevel.QUORUM); // for learn phase
              builder.setSerialConsistencyLevel(ConsistencyLevel.SERIAL); // for paxos phase
            });
  }

  protected BuildableQuery setCondition(BuildableQuery query, Mutation mutation) {
    if (mutation.getCondition().isPresent()) {
      ConditionSetter setter = new ConditionSetter(query);
      mutation.getCondition().get().accept(setter);
      return setter.getQuery();
    }
    return query;
  }

  protected void bindCondition(ValueBinder binder, Mutation mutation) {
    mutation
        .getCondition()
        .ifPresent(
            c -> {
              c.getExpressions().forEach(e -> e.getValue().accept(binder));
            });
  }
}
