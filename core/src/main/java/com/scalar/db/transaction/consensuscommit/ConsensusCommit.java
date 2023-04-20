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
import com.scalar.db.api.Selection;
import com.scalar.db.common.AbstractDistributedTransaction;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
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
  private Runnable beforeRecoveryHook;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ConsensusCommit(CrudHandler crud, CommitHandler commit, RecoveryHandler recovery) {
    this.crud = checkNotNull(crud);
    this.commit = checkNotNull(commit);
    this.recovery = checkNotNull(recovery);
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
    put = copyAndSetTargetToIfNot(put);
    crud.put(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkArgument(puts.size() != 0);
    puts.forEach(this::put);
  }

  @Override
  public void delete(Delete delete) {
    delete = copyAndSetTargetToIfNot(delete);
    crud.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) {
    checkArgument(deletes.size() != 0);
    deletes.forEach(this::delete);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) {
    checkArgument(mutations.size() != 0);
    mutations.forEach(
        m -> {
          if (m instanceof Put) {
            put((Put) m);
          } else if (m instanceof Delete) {
            delete((Delete) m);
          }
        });
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    commit.commit(crud.getSnapshot());
  }

  @Override
  public void rollback() {
    // do nothing for this implementation
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
    logger.debug("recover uncommitted records: {}", results);
    beforeRecoveryHook.run();
    results.forEach(r -> recovery.recover(selection, r));
  }
}
