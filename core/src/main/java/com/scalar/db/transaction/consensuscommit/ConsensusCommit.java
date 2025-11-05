package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractDistributedTransaction;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transaction manager that implements a transaction protocol on the basis of two-phase commit on
 * the consensus of an underlying storage.
 *
 * <p>When SERIALIZABLE is specified in {@link Isolation}, it makes schedule strict serializable or
 * serializable depending on underlying database operations. If a transaction runs on linearizable
 * operations, it makes it strict serializable. If a transaction runs on serializable operations, it
 * makes it serializable.
 *
 * <p>When SNAPSHOT is specified in {@link Isolation}, it makes it a weaker variant of snapshot
 * isolation (SI). This snapshot isolation could cause read skew anomalies in addition to write skew
 * and read-only anomalies, which are known to be usual SI anomalies.
 */
@NotThreadSafe
public class ConsensusCommit extends AbstractDistributedTransaction {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusCommit.class);
  private final TransactionContext context;
  private final CrudHandler crud;
  private final CommitHandler commit;
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;
  @Nullable private final CoordinatorGroupCommitter groupCommitter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ConsensusCommit(
      TransactionContext context,
      CrudHandler crud,
      CommitHandler commit,
      ConsensusCommitMutationOperationChecker mutationOperationChecker,
      @Nullable CoordinatorGroupCommitter groupCommitter) {
    this.context = checkNotNull(context);
    this.crud = checkNotNull(crud);
    this.commit = checkNotNull(commit);
    this.mutationOperationChecker = mutationOperationChecker;
    this.groupCommitter = groupCommitter;
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
    scan = copyAndSetTargetToIfNot(scan);
    return crud.getScanner(scan, context);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);
    checkMutation(put);
    crud.put(put, context);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    checkArgument(!puts.isEmpty());
    for (Put p : puts) {
      put(p);
    }
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    delete = copyAndSetTargetToIfNot(delete);
    checkMutation(delete);
    crud.delete(delete, context);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    checkArgument(!deletes.isEmpty());
    for (Delete d : deletes) {
      delete(d);
    }
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
  public void commit() throws CommitException, UnknownTransactionStatusException {
    if (!context.areAllScannersClosed()) {
      throw new IllegalStateException(CoreError.CONSENSUS_COMMIT_SCANNER_NOT_CLOSED.buildMessage());
    }

    // Execute implicit pre-read
    try {
      crud.readIfImplicitPreReadEnabled(context);
    } catch (CrudConflictException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHILE_IMPLICIT_PRE_READ.buildMessage(
              e.getMessage()),
          e,
          getId());
    } catch (CrudException e) {
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_EXECUTING_IMPLICIT_PRE_READ_FAILED.buildMessage(
              e.getMessage()),
          e,
          getId());
    }

    try {
      crud.waitForRecoveryCompletionIfNecessary(context);
    } catch (CrudConflictException e) {
      throw new CommitConflictException(e.getMessage(), e, getId());
    } catch (CrudException e) {
      throw new CommitException(e.getMessage(), e, getId());
    }

    commit.commit(context);
  }

  @Override
  public void rollback() {
    try {
      context.closeScanners();
    } catch (CrudException e) {
      logger.warn("Failed to close the scanner. Transaction ID: {}", getId(), e);
    }

    if (groupCommitter != null && !context.readOnly) {
      groupCommitter.remove(getId());
    }
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
