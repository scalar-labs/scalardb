package com.scalar.db.transaction.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractDistributedTransaction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RecordNotFoundException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.ScalarDbUtils;
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
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_GET_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);
    try {
      return jdbcService.scan(scan, connection);
    } catch (SQLException e) {
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_SCAN_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);

    try {
      if (!jdbcService.put(put, connection)) {
        throwUnsatisfiedConditionException(put);
      }
    } catch (SQLException e) {
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_PUT_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    delete = copyAndSetTargetToIfNot(delete);

    try {
      if (!jdbcService.delete(delete, connection)) {
        throwUnsatisfiedConditionException(delete);
      }
    } catch (SQLException e) {
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_DELETE_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    insert = copyAndSetTargetToIfNot(insert);

    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(insert.forNamespace().orElse(null))
            .table(insert.forTable().orElse(null))
            .partitionKey(insert.getPartitionKey());
    insert.getClusteringKey().ifPresent(buildable::clusteringKey);
    insert.getColumns().values().forEach(buildable::value);
    buildable.condition(ConditionBuilder.putIfNotExists());
    Put put = buildable.build();

    try {
      if (!jdbcService.put(put, connection)) {
        throw new CrudConflictException(
            CoreError.JDBC_TRANSACTION_CONFLICT_OCCURRED_IN_INSERT.buildMessage(), txId);
      }
    } catch (SQLException e) {
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_INSERT_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    upsert = copyAndSetTargetToIfNot(upsert);

    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(upsert.forNamespace().orElse(null))
            .table(upsert.forTable().orElse(null))
            .partitionKey(upsert.getPartitionKey());
    upsert.getClusteringKey().ifPresent(buildable::clusteringKey);
    upsert.getColumns().values().forEach(buildable::value);
    Put put = buildable.build();

    try {
      jdbcService.put(put, connection);
    } catch (SQLException e) {
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_UPSERT_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public void update(Update update) throws CrudException {
    update = copyAndSetTargetToIfNot(update);
    ScalarDbUtils.checkUpdate(update);

    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(update.forNamespace().orElse(null))
            .table(update.forTable().orElse(null))
            .partitionKey(update.getPartitionKey());
    update.getClusteringKey().ifPresent(buildable::clusteringKey);
    update.getColumns().values().forEach(buildable::value);
    if (update.getCondition().isPresent()) {
      buildable.condition(ConditionBuilder.putIf(update.getCondition().get().getExpressions()));
    } else {
      buildable.condition(ConditionBuilder.putIfExists());
    }
    Put put = buildable.build();

    try {
      if (!jdbcService.put(put, connection)) {
        if (update.getCondition().isPresent()) {
          throwUnsatisfiedConditionException(update);
        } else {
          throw new RecordNotFoundException(
              CoreError.JDBC_TRANSACTION_RECORD_NOT_FOUND.buildMessage(), txId);
        }
      }
    } catch (SQLException e) {
      throw createCrudException(
          e, CoreError.JDBC_TRANSACTION_UPDATE_OPERATION_FAILED.buildMessage(e.getMessage()));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      } else if (mutation instanceof Insert) {
        insert((Insert) mutation);
      } else if (mutation instanceof Upsert) {
        upsert((Upsert) mutation);
      } else {
        assert mutation instanceof Update;
        update((Update) mutation);
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
        throw new UnknownTransactionStatusException(
            CoreError.JDBC_TRANSACTION_UNKNOWN_TRANSACTION_STATUS.buildMessage(
                sqlException.getMessage()),
            sqlException,
            txId);
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
    assert mutation instanceof Put || mutation instanceof Delete || mutation instanceof Update;
    assert mutation.getCondition().isPresent();

    // Build the exception message
    MutationCondition condition = mutation.getCondition().get();
    String conditionColumns = null;
    // For PutIf, DeleteIf, and UpdateIf, aggregate the condition columns to the message
    if (condition instanceof PutIf
        || condition instanceof DeleteIf
        || condition instanceof UpdateIf) {
      List<ConditionalExpression> expressions = condition.getExpressions();
      conditionColumns =
          expressions.stream()
              .map(expr -> expr.getColumn().getName())
              .collect(Collectors.joining(", "));
    }

    throw new UnsatisfiedConditionException(
        CoreError.JDBC_TRANSACTION_CONDITION_NOT_SATISFIED.buildMessage(
            condition.getClass().getSimpleName(),
            mutation.getClass().getSimpleName(),
            conditionColumns == null ? "null" : "[" + conditionColumns + "]"),
        txId);
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
      throw new RollbackException(
          CoreError.JDBC_TRANSACTION_ROLLING_BACK_TRANSACTION_FAILED.buildMessage(e.getMessage()),
          e,
          txId);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.warn("Failed to close the connection", e);
      }
    }
  }

  private CrudException createCrudException(SQLException e, String message) {
    if (rdbEngine.isConflict(e)) {
      return new CrudConflictException(
          CoreError.JDBC_TRANSACTION_CONFLICT_OCCURRED.buildMessage(e.getMessage()), e, txId);
    }
    return new CrudException(message, e, txId);
  }

  private CommitException createCommitException(SQLException e) {
    if (rdbEngine.isConflict(e)) {
      return new CommitConflictException(
          CoreError.JDBC_TRANSACTION_CONFLICT_OCCURRED.buildMessage(e.getMessage()), e, txId);
    }
    return new CommitException(
        CoreError.JDBC_TRANSACTION_COMMITTING_TRANSACTION_FAILED.buildMessage(e.getMessage()),
        e,
        txId);
  }
}
