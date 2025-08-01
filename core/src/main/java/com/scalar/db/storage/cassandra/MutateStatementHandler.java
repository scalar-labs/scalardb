package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  public MutateStatementHandler(Session session) {
    super(session);
  }

  /**
   * Executes the specified {@link Mutation} {@link Operation}
   *
   * @param operation {@link Mutation} operation
   * @return a {@code ResultSet}
   * @throws ReadRepairableExecutionException if the execution partially fails, which can be
   *     repaired by a following read
   * @throws ExecutionException if the execution fails
   */
  @Override
  @Nonnull
  public ResultSet handle(Operation operation) throws ExecutionException {
    try {
      ResultSet results = handleInternal(operation);

      Mutation mutation = (Mutation) operation;
      if (mutation.getCondition().isPresent() && !results.one().getBool(0)) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(mutation));
      }
      return results;
    } catch (WriteTimeoutException e) {
      if (e.getWriteType() == WriteType.CAS) {
        throw new ExecutionException(
            CoreError.CASSANDRA_WRITE_TIMEOUT_IN_PAXOS_PHASE_IN_MUTATION.buildMessage(), e);
      } else if (e.getWriteType() == WriteType.SIMPLE) {
        Mutation mutation = (Mutation) operation;
        if (mutation.getCondition().isPresent()) {
          // learn phase needs to be repaired (by re-reading)
          throw new ReadRepairableExecutionException(
              CoreError.CASSANDRA_WRITE_TIMEOUT_IN_LEARN_PHASE_IN_MUTATION.buildMessage(), e);
        } else {
          throw new ExecutionException(
              CoreError.CASSANDRA_WRITE_TIMEOUT_SIMPLE_WRITE_OPERATION_FAILED_IN_MUTATION
                  .buildMessage(),
              e);
        }
      } else {
        throw new ExecutionException(
            CoreError.CASSANDRA_WRITE_TIMEOUT_WITH_OTHER_WRITE_TYPE_IN_MUTATION.buildMessage(), e);
      }
    } catch (RuntimeException e) {
      throw new ExecutionException(
          CoreError.CASSANDRA_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }

  @Override
  protected void overwriteConsistency(BoundStatement bound, Operation operation) {
    ((Mutation) operation)
        .getCondition()
        .ifPresent(
            c -> {
              bound.setConsistencyLevel(ConsistencyLevel.QUORUM); // for learn phase
              bound.setSerialConsistencyLevel(ConsistencyLevel.SERIAL); // for paxos phase
            });
  }

  protected void setCondition(BuiltStatement statement, Mutation mutation) {
    mutation.getCondition().ifPresent(c -> c.accept(new ConditionSetter(statement)));
  }

  protected void bindCondition(ValueBinder binder, Mutation mutation) {
    mutation
        .getCondition()
        .ifPresent(c -> c.getExpressions().forEach(e -> e.getColumn().accept(binder)));
  }
}
