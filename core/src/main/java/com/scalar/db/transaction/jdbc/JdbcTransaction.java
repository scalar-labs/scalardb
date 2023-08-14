package com.scalar.db.transaction.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.common.AbstractDistributedTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This indicates a transaction session of JDBC.
 *
 * @author Toshihiro Suzuki
 */
@NotThreadSafe
public class JdbcTransaction extends AbstractDistributedTransaction {
  private static final Logger logger = LoggerFactory.getLogger(JdbcTransaction.class);

  private final String txId;
  private final JdbcService jdbcService;
  private final Connection connection;
  private final RdbEngineStrategy rdbEngine;

  JdbcTransaction(
      String txId, JdbcService jdbcService, Connection connection, RdbEngineStrategy rdbEngine) {
    this.txId = txId;
    this.jdbcService = jdbcService;
    this.connection = connection;
    this.rdbEngine = rdbEngine;
  }

  @Override
  public String getId() {
    return txId;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    get = copyAndSetTargetToIfNot(get);
    try {
      return jdbcService.get(get, connection);
    } catch (SQLException e) {
      throw createCrudException(e, "Get operation failed");
    } catch (ExecutionException e) {
      throw new CrudException("Get operation failed", e, txId);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);
    try {
      return jdbcService.scan(scan, connection);
    } catch (SQLException e) {
      throw createCrudException(e, "Scan operation failed");
    } catch (ExecutionException e) {
      throw new CrudException("Scan operation failed", e, txId);
    }
  }

  @Override
  public void put(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);

    try {
      if (!jdbcService.put(put, connection)) {
        throwUnsatisfiedConditionException(put);
      }
    } catch (SQLException e) {
      throw createCrudException(e, "Put operation failed");
    } catch (ExecutionException e) {
      throw new CrudException("Put operation failed", e, txId);
    }
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    checkArgument(puts.size() != 0);
    for (Put put : puts) {
      put(put);
    }
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    delete = copyAndSetTargetToIfNot(delete);

    try {
      if (!jdbcService.delete(delete, connection)) {
        throwUnsatisfiedConditionException(delete);
      }
    } catch (SQLException e) {
      throw createCrudException(e, "Delete operation failed");
    } catch (ExecutionException e) {
      throw new CrudException("Delete operation failed", e, txId);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    checkArgument(deletes.size() != 0);
    for (Delete delete : deletes) {
      delete(delete);
    }
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(mutations.size() != 0);
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    try {
      connection.commit();
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException sqlException) {
        throw new UnknownTransactionStatusException("Failed to rollback", sqlException, txId);
      }
      throw createCommitException(e);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.warn("Failed to close the connection", e);
      }
    }
  }

  private void throwUnsatisfiedConditionException(Mutation mutation)
      throws UnsatisfiedConditionException {
    assert mutation.getCondition().isPresent();

    // Build the exception message
    MutationCondition condition = mutation.getCondition().get();
    String conditionColumns = null;
    // For PutIf and DeleteIf, aggregate the condition columns to the message
    if (condition instanceof PutIf || condition instanceof DeleteIf) {
      List<ConditionalExpression> expressions = condition.getExpressions();
      conditionColumns =
          expressions.stream()
              .map(expr -> expr.getColumn().getName())
              .collect(Collectors.joining(", "));
    }

    StringBuilder exceptionMessage =
        new StringBuilder("The ")
            .append(condition.getClass().getSimpleName())
            .append(" condition ");
    if (conditionColumns != null) {
      // To write 'column' in the plural or singular form
      if (condition.getExpressions().size() > 1) {
        exceptionMessage.append("targeting the columns '").append(conditionColumns).append("' ");
      } else {
        exceptionMessage.append("targeting the column '").append(conditionColumns).append("' ");
      }
    }
    exceptionMessage
        .append("of the ")
        .append(mutation.getClass().getSimpleName())
        .append(" operation is not satisfied");

    throw new UnsatisfiedConditionException(exceptionMessage.toString(), txId);
  }

  @Override
  public void rollback() throws RollbackException {
    try {
      if (connection.isClosed()) {
        // If the connection is already closed, do nothing here
        return;
      }

      connection.rollback();
    } catch (SQLException e) {
      throw new RollbackException("Failed to rollback", e, txId);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.warn("Failed to close the connection", e);
      }
    }
  }

  private CrudException createCrudException(SQLException e, String message) {
    if (rdbEngine.isConflictError(e)) {
      return new CrudConflictException("Conflict happened; try restarting transaction", e, txId);
    }
    return new CrudException(message, e, txId);
  }

  private CommitException createCommitException(SQLException e) {
    if (rdbEngine.isConflictError(e)) {
      return new CommitConflictException("Conflict happened; try restarting transaction", e, txId);
    }
    return new CommitException("Failed to commit", e, txId);
  }
}
