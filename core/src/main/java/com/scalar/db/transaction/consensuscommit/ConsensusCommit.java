package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.Utility;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transaction manager that implements a transaction protocol on the basis of two-phase commit on
 * the consensus of an underlining storage.
 *
 * <p>When SERIALIZABLE is specified in {@link com.scalar.db.api.Isolation}, it makes schedule
 * strict serializable or serializable depending on underlining database operations. If a
 * transaction runs on linearizable operations, it makes it strict serializable. If a transaction
 * runs on serializable operations, it makes it serializable.
 *
 * <p>When SNAPSHOT is specified in {@link com.scalar.db.api.Isolation}, it makes it a weaker
 * variant of snapshot isolation (SI). This snapshot isolation could cause read skew anomalies in
 * addition to write skew and read-only anomalies, which are known to be usual SI anomalies.
 */
@NotThreadSafe
public class ConsensusCommit implements DistributedTransaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusCommit.class);
  private final CrudHandler crud;
  private final CommitHandler commit;
  private final RecoveryHandler recovery;
  private Optional<String> namespace;
  private Optional<String> tableName;
  private Runnable beforeRecoveryHook;
  private Runnable beforeCommitHook;

  public ConsensusCommit(CrudHandler crud, CommitHandler commit, RecoveryHandler recovery) {
    this.crud = checkNotNull(crud);
    this.commit = checkNotNull(commit);
    this.recovery = checkNotNull(recovery);
    namespace = Optional.empty();
    tableName = Optional.empty();
    this.beforeRecoveryHook = () -> {};
    this.beforeCommitHook = () -> {};
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
    Utility.setTargetToIfNot(get, namespace, tableName);
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
    Utility.setTargetToIfNot(scan, namespace, tableName);
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
    Utility.setTargetToIfNot(put, namespace, tableName);
    crud.put(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkArgument(puts.size() != 0);
    puts.forEach(this::put);
  }

  @Override
  public void delete(Delete delete) {
    Utility.setTargetToIfNot(delete, namespace, tableName);
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
    beforeCommitHook.run();
    commit.commit(crud.getSnapshot());
  }

  @Override
  public void abort() {
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

  @VisibleForTesting
  void setBeforeCommitHook(Runnable beforeCommitHook) {
    this.beforeCommitHook = beforeCommitHook;
  }

  private void lazyRecovery(Selection selection, List<TransactionResult> results) {
    LOGGER.info("recover uncommitted record");
    beforeRecoveryHook.run();
    results.forEach(r -> recovery.recover(selection, r));
  }
}
