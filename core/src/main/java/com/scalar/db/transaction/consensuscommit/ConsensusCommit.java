package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.common.AbstractDistributedTransaction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.groupcommit.GroupCommitException;
import com.scalar.db.util.groupcommit.GroupCommitter;
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
  private final CrudHandler crud;
  private final CommitHandler commit;
  private final RecoveryHandler recovery;
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;
  @Nullable private final GroupCommitter<String, Snapshot> groupCommitter;
  private Runnable beforeRecoveryHook;

  // FIXME: For PoC
  public ConsensusCommit(
      CrudHandler crud,
      CommitHandler commit,
      RecoveryHandler recovery,
      ConsensusCommitMutationOperationChecker mutationOperationChecker) {
    this(crud, commit, recovery, mutationOperationChecker, null);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ConsensusCommit(
      CrudHandler crud,
      CommitHandler commit,
      RecoveryHandler recovery,
      ConsensusCommitMutationOperationChecker mutationOperationChecker,
      @Nullable GroupCommitter<String, Snapshot> groupCommitter) {
    this.crud = checkNotNull(crud);
    this.commit = checkNotNull(commit);
    this.recovery = checkNotNull(recovery);
    this.mutationOperationChecker = mutationOperationChecker;
    this.groupCommitter = groupCommitter;
    this.beforeRecoveryHook = () -> {};
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
    put = copyAndSetTargetToIfNot(put);
    checkMutation(put);
    crud.put(put);
  }

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
    crud.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    checkArgument(!deletes.isEmpty());
    for (Delete d : deletes) {
      delete(d);
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
      }
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    // Execute implicit pre-read
    try {
      crud.readIfImplicitPreReadEnabled();
    } catch (CrudConflictException e) {
      if (e instanceof UncommittedRecordException) {
        lazyRecovery((UncommittedRecordException) e);
      }
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHILE_IMPLICIT_PRE_READ.buildMessage(),
          e,
          getId());
    } catch (CrudException e) {
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_EXECUTING_IMPLICIT_PRE_READ_FAILED.buildMessage(), e, getId());
    }

    commit.commit(crud.getSnapshot());
  }

  @Override
  public void rollback() {
    // do nothing for this implementation
    // FIXME: For PoC
    if (groupCommitter != null) {
      try {
        groupCommitter.remove(crud.getSnapshot().getId());
      } catch (GroupCommitException e) {
        // FIXME: Change the exception type
        throw new RuntimeException(e);
      }
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
