package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.AbstractTwoPhaseCommitTransaction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
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
      lazyRecovery(e);
      throw e;
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);
    try {
      return crud.scan(scan);
    } catch (UncommittedRecordException e) {
      lazyRecovery(e);
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
    try {
      crud.put(put);
    } catch (UncommittedRecordException e) {
      lazyRecovery(e);
      throw e;
    }
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
    try {
      crud.delete(delete);
    } catch (UncommittedRecordException e) {
      lazyRecovery(e);
      throw e;
    }
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
      if (e instanceof UncommittedRecordException) {
        lazyRecovery((UncommittedRecordException) e);
      }
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHILE_IMPLICIT_PRE_READ.buildMessage(),
          e,
          getId());
    } catch (CrudException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_EXECUTING_IMPLICIT_PRE_READ_FAILED.buildMessage(), e, getId());
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
  public void commit() throws CommitConflictException, UnknownTransactionStatusException {
    if (crud.getSnapshot().isValidationRequired() && !validated) {
      throw new IllegalStateException(
          CoreError.CONSENSUS_COMMIT_TRANSACTION_NOT_VALIDATED_IN_EXTRA_READ.buildMessage());
    }

    try {
      commit.commitState(crud.getSnapshot());
    } catch (CommitConflictException | UnknownTransactionStatusException e) {
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
            CoreError.CONSENSUS_COMMIT_ROLLBACK_FAILED_BECAUSE_TRANSACTION_ALREADY_COMMITTED
                .buildMessage(),
            getId());
      }
    } catch (UnknownTransactionStatusException e) {
      throw new RollbackException(
          CoreError.CONSENSUS_COMMIT_ROLLBACK_FAILED.buildMessage(), e, getId());
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

  private void lazyRecovery(UncommittedRecordException e) {
    logger.debug("Recover uncommitted records: {}", e.getResults());
    beforeRecoveryHook.run();
    e.getResults().forEach(r -> recovery.recover(e.getSelection(), r));
  }

  private void checkMutation(Mutation mutation) throws CrudException {
    try {
      mutationOperationChecker.check(mutation);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, getId());
    }
  }
}
