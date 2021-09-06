package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PrepareConflictException;
import com.scalar.db.exception.transaction.PrepareException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.util.Utility;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoPhaseConsensusCommit implements TwoPhaseCommit {
  private static final Logger LOGGER = LoggerFactory.getLogger(TwoPhaseConsensusCommit.class);

  @VisibleForTesting
  enum Status {
    ACTIVE,
    PREPARED,
    PREPARE_FAILED,
    VALIDATED,
    VALIDATION_FAILED,
    COMMITTED,
    COMMIT_FAILED,
    ROLLBACKED
  }

  private final CrudHandler crud;
  private final CommitHandler commit;
  private final RecoveryHandler recovery;
  private final boolean isCoordinator;
  private final TwoPhaseConsensusCommitManager manager;

  private Optional<String> namespace = Optional.empty();
  private Optional<String> tableName = Optional.empty();

  @VisibleForTesting Status status;

  // For test
  private Runnable beforeRecoveryHook = () -> {};
  private Runnable beforePrepareHook = () -> {};

  public TwoPhaseConsensusCommit(
      CrudHandler crud,
      CommitHandler commit,
      RecoveryHandler recovery,
      boolean isCoordinator,
      TwoPhaseConsensusCommitManager manager) {
    this.crud = crud;
    this.commit = commit;
    this.recovery = recovery;
    this.isCoordinator = isCoordinator;
    this.manager = manager;

    status = Status.ACTIVE;
  }

  @Override
  public String getId() {
    return crud.getSnapshot().getId();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  /**
   * Retrieves a result from the storage through a transaction with the specified {@link Get}
   * command with a primary key and returns the result. Note that the current implementation clears
   * the specified projections in {@link Get} to project all the values including metadata properly,
   * but the behavior might be changed at some point.
   *
   * @param get a {@code Get} command
   * @return an {@code Optional} with the returned result
   * @throws CrudException if the operation failed
   */
  @Override
  public Optional<Result> get(Get get) throws CrudException {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    setTargetToIfNot(get);
    get.clearProjections(); // project all
    try {
      return crud.get(get);
    } catch (UncommittedRecordException e) {
      lazyRecovery(get, e.getResults());
      throw e;
    }
  }

  /**
   * Retrieves results from the storage through a transaction with the specified {@link Scan}
   * command with a partition key and returns a list of {@link Result}. Results can be filtered by
   * specifying a range of clustering keys. Note that the current implementation clears the
   * specified projections in {@link Scan} to project all the values including metadata properly,
   * but the behavior might be changed at some point.
   *
   * @param scan a {@code Scan} command
   * @return a list of {@link Result}
   * @throws CrudException if the operation failed
   */
  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    setTargetToIfNot(scan);
    scan.clearProjections(); // project all
    try {
      return crud.scan(scan);
    } catch (UncommittedRecordException e) {
      lazyRecovery(scan, e.getResults());
      throw e;
    }
  }

  @Override
  public void put(Put put) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    putInternal(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    checkArgument(puts.size() != 0);
    puts.forEach(this::putInternal);
  }

  private void putInternal(Put put) {
    setTargetToIfNot(put);
    crud.put(put);
  }

  @Override
  public void delete(Delete delete) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    deleteInternal(delete);
  }

  @Override
  public void delete(List<Delete> deletes) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    checkArgument(deletes.size() != 0);
    deletes.forEach(this::deleteInternal);
  }

  private void deleteInternal(Delete delete) {
    setTargetToIfNot(delete);
    crud.delete(delete);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    updateTransactionExpirationTime();
    checkArgument(mutations.size() != 0);
    mutations.forEach(
        m -> {
          if (m instanceof Put) {
            putInternal((Put) m);
          } else if (m instanceof Delete) {
            deleteInternal((Delete) m);
          }
        });
  }

  @Override
  public void prepare() throws PrepareException {
    checkStatus("The transaction is not active", Status.ACTIVE);
    beforePrepareHook.run();
    updateTransactionExpirationTime();

    try {
      commit.prepare(crud.getSnapshot(), false);
      status = Status.PREPARED;
    } catch (CommitConflictException e) {
      status = Status.PREPARE_FAILED;
      throw new PrepareConflictException("prepare failed", e);
    } catch (CommitException e) {
      status = Status.PREPARE_FAILED;
      throw new PrepareException("prepare failed", e);
    } catch (UnknownTransactionStatusException ignored) {
      // UnknownTransactionStatusException must not be thrown here
    }
  }

  @Override
  public void validate() throws ValidationException {
    checkStatus("The transaction is not prepared", Status.PREPARED);
    updateTransactionExpirationTime();

    try {
      commit.preCommitValidation(crud.getSnapshot(), false);
      status = Status.VALIDATED;
    } catch (CommitConflictException e) {
      status = Status.VALIDATION_FAILED;
      throw new ValidationConflictException("validation failed", e);
    } catch (CommitException e) {
      status = Status.VALIDATION_FAILED;
      throw new ValidationException("validation failed", e);
    } catch (UnknownTransactionStatusException ignored) {
      // UnknownTransactionStatusException must not be thrown here
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    if (crud.getSnapshot().getIsolation() == Isolation.SERIALIZABLE
        && crud.getSnapshot().getSerializableStrategy() == SerializableStrategy.EXTRA_READ) {
      checkStatus(
          "The transaction is not validated."
              + " When using the EXTRA_READ serializable strategy, you need to call validate()"
              + " before calling commit()",
          Status.VALIDATED);
    } else {
      checkStatus(
          "The transaction is not prepared or validated.", Status.PREPARED, Status.VALIDATED);
    }

    try {
      if (isCoordinator) {
        commit.commitState(crud.getSnapshot());
      }

      commit.commitRecords(crud.getSnapshot());
      status = Status.COMMITTED;
    } catch (CommitException e) {
      status = Status.COMMIT_FAILED;
      throw e;
    } finally {
      if (!isCoordinator) {
        manager.removeTransaction(getId());
      }
    }
  }

  @Override
  public void rollback() throws RollbackException {
    if (status == Status.COMMITTED || status == Status.ROLLBACKED) {
      throw new IllegalStateException("The transaction has already been committed or rollbacked");
    }

    try {
      if (status == Status.COMMIT_FAILED || status == Status.ACTIVE) {
        // If the status is COMMIT_FAILED, the transaction has already been aborted, so do nothing.
        // And if the status is ACTIVE, it means that the transaction needs to be rollbacked
        // before it's prepared. We do nothing in this case.
        return;
      }

      if (isCoordinator) {
        try {
          commit.abort(crud.getSnapshot().getId());
        } catch (UnknownTransactionStatusException e) {
          throw new RollbackException("rollback failed", e);
        }
      }

      commit.rollbackRecords(crud.getSnapshot());
    } finally {
      if (!isCoordinator) {
        manager.removeTransaction(getId());
      }
      status = Status.ROLLBACKED;
    }
  }

  @VisibleForTesting
  CrudHandler getCrudHandler() {
    return crud;
  }

  @VisibleForTesting
  CommitHandler getCommitHandler() {
    return commit;
  }

  @VisibleForTesting
  RecoveryHandler getRecoveryHandler() {
    return recovery;
  }

  @VisibleForTesting
  void setBeforeRecoveryHook(Runnable beforeRecoveryHook) {
    this.beforeRecoveryHook = beforeRecoveryHook;
  }

  @VisibleForTesting
  void setBeforePrepareHook(Runnable beforePrepareHook) {
    this.beforePrepareHook = beforePrepareHook;
  }

  private void checkStatus(@Nullable String message, Status... expectedStatus) {
    boolean expected = Arrays.stream(expectedStatus).anyMatch(s -> status == s);
    if (!expected) {
      throw new IllegalStateException(message);
    }
  }

  private void updateTransactionExpirationTime() {
    if (!isCoordinator) {
      manager.updateTransactionExpirationTime(crud.getSnapshot().getId());
    }
  }

  private void lazyRecovery(Selection selection, List<TransactionResult> results) {
    LOGGER.info("recover uncommitted record");
    beforeRecoveryHook.run();
    results.forEach(r -> recovery.recover(selection, r));
  }

  private void setTargetToIfNot(Operation operation) {
    Utility.setTargetToIfNot(operation, namespace, tableName);
  }
}
