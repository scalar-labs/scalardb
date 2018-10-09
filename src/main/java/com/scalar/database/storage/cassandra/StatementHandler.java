package com.scalar.database.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Selection;
import com.scalar.database.exception.storage.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatementHandler.class);
  protected final Session session;
  protected StatementCache cache;

  protected StatementHandler(Session session) {
    this.session = checkNotNull(session);
    cache = new StatementCache();
  }

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation {@code Operation} to execute
   * @return {@code ResultSet}
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

  @Nonnull
  protected ResultSet handleInternal(Operation operation) {
    PreparedStatement prepared = prepare(operation);
    BoundStatement bound = bind(prepared, operation);
    setConsistency(bound, operation);
    return execute(bound, operation);
  }

  @Nonnull
  protected PreparedStatement prepare(String queryString) {
    LOGGER.debug("query to prepare : [" + queryString + "].");
    PreparedStatement prepared = cache.get(queryString);
    if (prepared == null) {
      prepared = session.prepare(queryString);
      cache.put(queryString, prepared);
    } else {
      LOGGER.debug("there was a hit in the statement cache for [" + queryString + "].");
    }
    return prepared;
  }

  protected void setConsistency(BoundStatement bound, Operation operation) {
    bound.setConsistencyLevel(StatementHandler.convert(operation, operation.getConsistency()));
    // set preferable consistency for the operation
    overwriteConsistency(bound, operation);
  }

  protected abstract PreparedStatement prepare(Operation operation);

  protected abstract BoundStatement bind(PreparedStatement prepared, Operation operation);

  protected abstract ResultSet execute(BoundStatement bound, Operation operation);

  protected abstract void overwriteConsistency(BoundStatement bound, Operation operation);

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
