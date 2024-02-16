package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A handler class for statements */
@ThreadSafe
public abstract class StatementHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatementHandler.class);
  protected final Session session;
  protected final StatementCache cache;

  /**
   * Constructs a {@code StatementHandler} with the specified {@link Session} and a new {@link
   * StatementCache}
   *
   * @param session {@code Session}
   */
  protected StatementHandler(Session session) {
    this.session = checkNotNull(session);
    cache = new StatementCache();
  }

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation an {@code Operation} to execute
   * @return a {@code ResultSet}
   * @throws ExecutionException if the execution fails
   */
  @Nonnull
  public abstract ResultSet handle(Operation operation) throws ExecutionException;

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation an {@code Operation} to execute
   * @return a {@code ResultSet}
   */
  @Nonnull
  protected ResultSet handleInternal(Operation operation) {
    PreparedStatement prepared = prepare(operation);
    BoundStatement bound = bind(prepared, operation);
    setConsistency(bound, operation);
    return execute(bound, operation);
  }

  /**
   * Returns a {@link PreparedStatement} based on the given query string
   *
   * @param queryString a query string
   * @return a {@code PreparedStatement}
   */
  @Nonnull
  protected PreparedStatement prepare(String queryString) {
    PreparedStatement prepared = cache.get(queryString);
    if (prepared == null) {
      prepared = session.prepare(queryString);
      cache.put(queryString, prepared);
    }
    return prepared;
  }

  /**
   * Sets the consistency level for the specified {@link BoundStatement} and {@link Operation}
   *
   * @param bound a {@code BoundStatement}
   * @param operation an {@code Operation}
   */
  protected void setConsistency(BoundStatement bound, Operation operation) {
    bound.setConsistencyLevel(StatementHandler.convert(operation, operation.getConsistency()));
    // set preferable consistency for the operation
    overwriteConsistency(bound, operation);
  }

  protected abstract PreparedStatement prepare(Operation operation);

  protected abstract BoundStatement bind(PreparedStatement prepared, Operation operation);

  protected abstract ResultSet execute(BoundStatement bound, Operation operation);

  protected abstract void overwriteConsistency(BoundStatement bound, Operation operation);

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
        logger.warn("Unsupported consistency is specified. SEQUENTIAL is being used instead");
        return ConsistencyLevel.QUORUM;
    }
  }

  @SafeVarargs
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
                  expected[0].toString(), "is expected"
                }));
  }
}
