package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractTwoPhaseCommitTransaction;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class TwoPhaseConsensusCommit extends AbstractTwoPhaseCommitTransaction {
  private static final Logger logger = LoggerFactory.getLogger(TwoPhaseConsensusCommit.class);
  private final TransactionContext context;
  private final CrudHandler crud;
  private final CommitHandler commit;
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;
  private boolean validated;
  private boolean needRollback;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseConsensusCommit(
      TransactionContext context,
      CrudHandler crud,
      CommitHandler commit,
      ConsensusCommitMutationOperationChecker mutationOperationChecker) {
    this.context = context;
    this.crud = crud;
    this.commit = commit;
    this.mutationOperationChecker = mutationOperationChecker;
  }

  @Override
  public String getId() {
    return context.transactionId;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return crud.get(copyAndSetTargetToIfNot(get), context);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return crud.scan(copyAndSetTargetToIfNot(scan), context);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return crud.getScanner(copyAndSetTargetToIfNot(scan), context);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    putInternal(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
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
    crud.put(put, context);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    deleteInternal(delete);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
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
    crud.delete(delete, context);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    insert = copyAndSetTargetToIfNot(insert);
    Put put = ConsensusCommitUtils.createPutForInsert(insert);
    checkMutation(put);
    crud.put(put, context);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    upsert = copyAndSetTargetToIfNot(upsert);
    Put put = ConsensusCommitUtils.createPutForUpsert(upsert);
    checkMutation(put);
    crud.put(put, context);
  }

  @Override
  public void update(Update update) throws CrudException {
    update = copyAndSetTargetToIfNot(update);
    ScalarDbUtils.checkUpdate(update);
    Put put = ConsensusCommitUtils.createPutForUpdate(update);
    checkMutation(put);
    try {
      crud.put(put, context);
    } catch (UnsatisfiedConditionException e) {
      if (update.getCondition().isPresent()) {
        throw new UnsatisfiedConditionException(
            ConsensusCommitUtils.convertUnsatisfiedConditionExceptionMessageForUpdate(
                e, update.getCondition().get()),
            getId());
      }

      // If the condition is not specified, it means that the record does not exist. In this case,
      // we do nothing
    }
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(!mutations.isEmpty());
    for (Mutation m : mutations) {
      if (m instanceof Put) {
        put((Put) m);
      } else if (m instanceof Delete) {
        delete((Delete) m);
      } else if (m instanceof Insert) {
        insert((Insert) m);
      } else if (m instanceof Upsert) {
        upsert((Upsert) m);
      } else {
        assert m instanceof Update;
        update((Update) m);
      }
    }
  }

  @Override
  public void prepare() throws PreparationException {
    if (!context.areAllScannersClosed()) {
      throw new IllegalStateException(
          CoreError.TWO_PHASE_CONSENSUS_COMMIT_SCANNER_NOT_CLOSED.buildMessage());
    }

    // Execute implicit pre-read
    try {
      crud.readIfImplicitPreReadEnabled(context);
    } catch (CrudConflictException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHILE_IMPLICIT_PRE_READ.buildMessage(
              e.getMessage()),
          e,
          getId());
    } catch (CrudException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_EXECUTING_IMPLICIT_PRE_READ_FAILED.buildMessage(
              e.getMessage()),
          e,
          getId());
    }

    try {
      crud.waitForRecoveryCompletionIfNecessary(context);
    } catch (CrudConflictException e) {
      throw new PreparationConflictException(e.getMessage(), e, getId());
    } catch (CrudException e) {
      throw new PreparationException(e.getMessage(), e, getId());
    }

    try {
      commit.prepareRecords(context);
    } finally {
      needRollback = true;
    }
  }

  @Override
  public void validate() throws ValidationException {
    commit.validateRecords(context);
    validated = true;
  }

  @Override
  public void commit() throws CommitConflictException, UnknownTransactionStatusException {
    if (context.isValidationRequired() && !validated) {
      throw new IllegalStateException(
          CoreError.CONSENSUS_COMMIT_TRANSACTION_NOT_VALIDATED_IN_SERIALIZABLE.buildMessage());
    }

    try {
      commit.commitState(context);
    } catch (CommitConflictException | UnknownTransactionStatusException e) {
      // no need to rollback because the transaction has already been rolled back
      needRollback = false;

      throw e;
    }

    commit.commitRecords(context);
  }

  @Override
  public void rollback() throws RollbackException {
    try {
      context.closeScanners();
    } catch (CrudException e) {
      logger.warn("Failed to close the scanner. Transaction ID: {}", getId(), e);
    }

    if (!needRollback) {
      return;
    }

    try {
      TransactionState state = commit.abortState(getId());
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

    commit.rollbackRecords(context);
  }

  @VisibleForTesting
  TransactionContext getTransactionContext() {
    return context;
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
  void waitForRecoveryCompletion() throws CrudException {
    crud.waitForRecoveryCompletion(context);
  }

  private void checkMutation(Mutation mutation) throws CrudException {
    try {
      mutationOperationChecker.check(mutation);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, getId());
    }
  }
}
