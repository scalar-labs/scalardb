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
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.AbstractTwoPhaseCommitTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
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
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;
  private boolean validated;
  private boolean needRollback;

  // For test
  private Runnable beforeRecoveryHook = () -> {};

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseConsensusCommit(
      CrudHandler crud,
      CommitHandler commit,
      RecoveryHandler recovery,
      ConsensusCommitMutationOperationChecker mutationOperationChecker) {
    this.crud = crud;
    this.commit = commit;
    this.recovery = recovery;
    this.mutationOperationChecker = mutationOperationChecker;
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
  public void put(Put put) throws CrudException {
    putInternal(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    checkArgument(!puts.isEmpty());
    for (Put p : puts) {
      putInternal(p);
    }
  }

  private void putInternal(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);
    checkMutation(put);
    crud.put(put);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    deleteInternal(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    checkArgument(!deletes.isEmpty());
    for (Delete d : deletes) {
      deleteInternal(d);
    }
  }

  private void deleteInternal(Delete delete) throws CrudException {
    delete = copyAndSetTargetToIfNot(delete);
    checkMutation(delete);
    crud.delete(delete);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(!mutations.isEmpty());
    for (Mutation m : mutations) {
      if (m instanceof Put) {
        putInternal((Put) m);
      } else if (m instanceof Delete) {
        deleteInternal((Delete) m);
      }
    }
  }

  @Override
  public void prepare() throws PreparationException {
    // Execute implicit pre-read
    try {
      crud.readIfImplicitPreReadEnabled();
    } catch (CrudConflictException e) {
      throw new PreparationConflictException(
          "Conflict occurred while implicit pre-read", e, getId());
    } catch (CrudException e) {
      throw new PreparationException("Failed to execute implicit pre-read", e, getId());
    }

    try {
      commit.prepare(crud.getSnapshot());
    } finally {
      needRollback = true;
    }
  }

  @Override
  public void validate() throws ValidationException {
    commit.validate(crud.getSnapshot());
    validated = true;
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    if (crud.getSnapshot().isValidationRequired() && !validated) {
      throw new IllegalStateException(
          "The transaction is not validated."
              + " When using the EXTRA_READ serializable strategy, you need to call validate()"
              + " before calling commit()");
    }

    try {
      commit.commitState(crud.getSnapshot());
    } catch (CommitException | UnknownTransactionStatusException e) {
      // no need to rollback because the transaction has already been rolled back
      needRollback = false;

      throw e;
    }

    commit.commitRecords(crud.getSnapshot());
  }

  @Override
  public void rollback() throws RollbackException {
    if (!needRollback) {
      return;
    }

    try {
      TransactionState state = commit.abortState(crud.getSnapshot().getId());
      if (state == TransactionState.COMMITTED) {
        throw new RollbackException(
            "Rollback failed because the transaction has already been committed", getId());
      }
    } catch (UnknownTransactionStatusException e) {
      throw new RollbackException("Rollback failed", e, getId());
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
    logger.debug("Recover uncommitted records: {}", results);
    beforeRecoveryHook.run();
    results.forEach(r -> recovery.recover(selection, r));
  }

  private void checkMutation(Mutation mutation) throws CrudException {
    try {
      mutationOperationChecker.check(mutation);
    } catch (ExecutionException e) {
      throw new CrudException("Checking the operation failed", e, getId());
    }
  }
}
