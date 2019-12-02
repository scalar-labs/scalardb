package com.scalar.database.storage.cassandra4driver;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.base.Joiner;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Selection;
import com.scalar.database.exception.storage.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A handler class for statements */
@ThreadSafe
public abstract class StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatementHandler.class);
  protected final CqlSession session;

  /**
   * Constructs a {@code StatementHandler} with the specified {@link CqlSession}
   *
   * @param session {@code CqlSession}
   */
  protected StatementHandler(CqlSession session) {
    this.session = checkNotNull(session);
  }

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation an {@code Operation} to execute
   * @return a {@code ResultSet}
   * @throws ExecutionException if the execution failed
   */
  @Nonnull
  public ResultSet handle(Operation operation) throws ExecutionException {
    try {
      LOGGER.debug(operation + " is started.");
      ResultSet results = handleInternal(operation);
      LOGGER.debug(operation + " is executed normally.");
      return results;

    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation an {@code Operation} to execute
   * @return a {@code ResultSet}
   */
  @Nonnull
  protected ResultSet handleInternal(Operation operation) {
    PreparedStatement prepared = prepare(operation);
    BoundStatementBuilder builder = bind(prepared, operation);
    setConsistency(builder, operation);
    return execute(builder.build(), operation);
  }

  /**
   * Returns a {@link PreparedStatement} based on the given query string
   *
   * @param queryString
   * @return a {@code PreparedStatement}
   */
  @Nonnull
  protected PreparedStatement prepare(String queryString) {
    LOGGER.debug("query to prepare : [" + queryString + "].");
    return session.prepare(queryString);
  }

  /**
   * Sets the consistency level for the specified {@link BoundStatementBuilder} and {@link Operation}
   *
   * @param builder a {@code BoundStatementBuilder}
   * @param operation an {@code Operation}
   */
  protected void setConsistency(BoundStatementBuilder builder, Operation operation) {
    builder.setConsistencyLevel(StatementHandler.convert(operation, operation.getConsistency()));
    // set preferable consistency for the operation
    overwriteConsistency(builder, operation);
  }

  protected abstract PreparedStatement prepare(Operation operation);

  protected abstract BoundStatementBuilder bind(PreparedStatement prepared, Operation operation);

  protected abstract ResultSet execute(BoundStatement bound, Operation operation);

  protected abstract void overwriteConsistency(BoundStatementBuilder builder, Operation operation);

  /**
   * Returns a {@link ConsistencyLevel} based on the specified {@link Operation} and {@link
   * Consistency}
   *
   * @param operation an {@code Operation}
   * @param consistency a {@code Consistency}
   * @return a {@code ConsistencyLevel}
   */
  public static ConsistencyLevel convert(Operation operation, Consistency consistency) {
    switch (consistency) {
      case SEQUENTIAL:
        return ConsistencyLevel.QUORUM;
      case EVENTUAL:
        return ConsistencyLevel.ONE;
      case LINEARIZABLE:
        if (operation instanceof Selection) {
          // setConsistencyLevel() with SERIAL is only valid when an operation is read.
          // when an operation is write, it comes with conditional update and consistency level is
          // automatically set in overwriteConsistency().
          return ConsistencyLevel.SERIAL;
        } else {
          return ConsistencyLevel.QUORUM;
        }
      default:
        LOGGER.warn("Unsupported consistency is specified. SEQUENTIAL is being used instead.");
        return ConsistencyLevel.QUORUM;
    }
  }

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
}
