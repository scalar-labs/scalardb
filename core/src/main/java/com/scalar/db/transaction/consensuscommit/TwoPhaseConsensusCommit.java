package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.common.AbstractTwoPhaseCommitTransaction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class TwoPhaseConsensusCommit extends AbstractTwoPhaseCommitTransaction {
  private static final Logger logger = LoggerFactory.getLogger(TwoPhaseConsensusCommit.class);

  @VisibleForTesting
  enum Status {
    ACTIVE,
    PREPARED,
    PREPARE_FAILED,
    VALIDATED,
    VALIDATION_FAILED,
    COMMITTED,
    COMMIT_FAILED,
    ROLLED_BACK
  }

  private final CrudHandler crud;
  private final CommitHandler commit;
  private final RecoveryHandler recovery;
  private final boolean isCoordinator;

  @VisibleForTesting Status status;

  // For test
  private Runnable beforeRecoveryHook = () -> {};

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseConsensusCommit(
      CrudHandler crud,
      CommitHandler commit,
      RecoveryHandler recovery,
      boolean isCoordinator,
      TwoPhaseConsensusCommitManager manager) {
    super(manager);
    this.crud = crud;
    this.commit = commit;
    this.recovery = recovery;
    this.isCoordinator = isCoordinator;

    status = Status.ACTIVE;
  }

  @Override
  public String getId() {
    return crud.getSnapshot().getId();
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    checkStatus("The transaction is not active", Status.ACTIVE);
    get = copyAndSetTargetToIfNot(get);
    try {
      return crud.get(get);
    } catch (UncommittedRecordException e) {
      lazyRecovery(get, e.getResults());
      throw e;
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    checkStatus("The transaction is not active", Status.ACTIVE);
    scan = copyAndSetTargetToIfNot(scan);
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
    putInternal(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    checkArgument(puts.size() != 0);
    puts.forEach(this::putInternal);
  }

  private void putInternal(Put put) {
    put = copyAndSetTargetToIfNot(put);
    crud.put(put);
  }

  @Override
  public void delete(Delete delete) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    deleteInternal(delete);
  }

  @Override
  public void delete(List<Delete> deletes) {
    checkStatus("The transaction is not active", Status.ACTIVE);
    checkArgument(deletes.size() != 0);
    deletes.forEach(this::deleteInternal);
  }

  private void deleteInternal(Delete delete) {
    delete = copyAndSetTargetToIfNot(delete);
    crud.delete(delete);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) {
    checkStatus("The transaction is not active", Status.ACTIVE);
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
  public void prepare() throws PreparationException {
    checkStatus("The transaction is not active", Status.ACTIVE);

    try {
      commit.prepare(crud.getSnapshot(), false);
      status = Status.PREPARED;
    } catch (CommitConflictException e) {
      status = Status.PREPARE_FAILED;
      throw new PreparationConflictException("prepare failed", e);
    } catch (CommitException e) {
      status = Status.PREPARE_FAILED;
      throw new PreparationException("prepare failed", e);
    } catch (UnknownTransactionStatusException e) {
      // Should not be reached here because CommitHandler.prepare() with abortIfError=false won't
      // throw UnknownTransactionStatusException
    }
  }

  @Override
  public void validate() throws ValidationException {
    checkStatus("The transaction is not prepared", Status.PREPARED);

    try {
      commit.preCommitValidation(crud.getSnapshot(), false);
      status = Status.VALIDATED;
    } catch (CommitConflictException e) {
      status = Status.VALIDATION_FAILED;
      throw new ValidationConflictException("validation failed", e);
    } catch (CommitException e) {
      status = Status.VALIDATION_FAILED;
      throw new ValidationException("validation failed", e);
    } catch (UnknownTransactionStatusException e) {
      // Should not be reached here because CommitHandler.prepare() with abortIfError=false won't
      // throw UnknownTransactionStatusException
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    if (crud.getSnapshot().isPreCommitValidationRequired()) {
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
      removeActiveTransaction(getId());
    } catch (CommitException e) {
      status = Status.COMMIT_FAILED;
      throw e;
    }
  }

  @Override
  public void rollback() throws RollbackException {
    if (status == Status.COMMITTED || status == Status.ROLLED_BACK) {
      throw new IllegalStateException("The transaction has already been committed or rolled back");
    }

    try {
      if (status == Status.COMMIT_FAILED || status == Status.ACTIVE) {
        // If the status is COMMIT_FAILED, the transaction has already been aborted, so do nothing.
        // And if the status is ACTIVE, it means that the transaction needs to be rolled back
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
      status = Status.ROLLED_BACK;
      removeActiveTransaction(getId());
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

  private void checkStatus(@Nullable String message, Status... expectedStatus) {
    boolean expected = Arrays.stream(expectedStatus).anyMatch(s -> status == s);
    if (!expected) {
      throw new IllegalStateException(message);
    }
  }

  private void lazyRecovery(Selection selection, List<TransactionResult> results) {
    logger.debug("recover uncommitted records: " + results);
    beforeRecoveryHook.run();
    results.forEach(r -> recovery.recover(selection, r));
  }
}
