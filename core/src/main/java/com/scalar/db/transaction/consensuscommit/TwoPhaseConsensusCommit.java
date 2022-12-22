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
import com.scalar.db.common.AbstractTwoPhaseCommitTransaction;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class TwoPhaseConsensusCommit extends AbstractTwoPhaseCommitTransaction {
  private static final Logger logger = LoggerFactory.getLogger(TwoPhaseConsensusCommit.class);

  private final CrudHandler crud;
  private final CommitHandler commit;
  private final RecoveryHandler recovery;
  private final boolean isCoordinator;

  private boolean validated;
  private boolean needRollback;

  // For test
  private Runnable beforeRecoveryHook = () -> {};

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseConsensusCommit(
      CrudHandler crud, CommitHandler commit, RecoveryHandler recovery, boolean isCoordinator) {
    this.crud = crud;
    this.commit = commit;
    this.recovery = recovery;
    this.isCoordinator = isCoordinator;
  }

  @Override
  public String getId() {
    return crud.getSnapshot().getId();
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
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
    putInternal(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkArgument(puts.size() != 0);
    puts.forEach(this::putInternal);
  }

  private void putInternal(Put put) {
    put = copyAndSetTargetToIfNot(put);
    crud.put(put);
  }

  @Override
  public void delete(Delete delete) {
    deleteInternal(delete);
  }

  @Override
  public void delete(List<Delete> deletes) {
    checkArgument(deletes.size() != 0);
    deletes.forEach(this::deleteInternal);
  }

  private void deleteInternal(Delete delete) {
    delete = copyAndSetTargetToIfNot(delete);
    crud.delete(delete);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) {
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
    try {
      commit.prepare(crud.getSnapshot(), false);
    } catch (CommitConflictException e) {
      throw new PreparationConflictException("prepare failed", e);
    } catch (CommitException e) {
      throw new PreparationException("prepare failed", e);
    } catch (UnknownTransactionStatusException e) {
      // Should not be reached here because CommitHandler.prepare() with abortIfError=false won't
      // throw UnknownTransactionStatusException
    } finally {
      needRollback = true;
    }
  }

  @Override
  public void validate() throws ValidationException {
    try {
      commit.preCommitValidation(crud.getSnapshot(), false);
      validated = true;
    } catch (CommitConflictException e) {
      throw new ValidationConflictException("validation failed", e);
    } catch (CommitException e) {
      throw new ValidationException("validation failed", e);
    } catch (UnknownTransactionStatusException e) {
      // Should not be reached here because CommitHandler.prepare() with abortIfError=false won't
      // throw UnknownTransactionStatusException
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    if (crud.getSnapshot().isPreCommitValidationRequired() && !validated) {
      throw new IllegalStateException(
          "The transaction is not validated."
              + " When using the EXTRA_READ serializable strategy, you need to call validate()"
              + " before calling commit()");
    }

    if (isCoordinator) {
      try {
        commit.commitState(crud.getSnapshot());
      } catch (Exception e) {
        // no need to rollback because the transaction has already been rolled back
        needRollback = false;

        throw e;
      }
    }

    commit.commitRecords(crud.getSnapshot());
  }

  @Override
  public void rollback() throws RollbackException {
    if (!needRollback) {
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

  private void lazyRecovery(Selection selection, List<TransactionResult> results) {
    logger.debug("recover uncommitted records: " + results);
    beforeRecoveryHook.run();
    results.forEach(r -> recovery.recover(selection, r));
  }
}
