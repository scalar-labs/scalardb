package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO : make it thread-safe
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
  public Optional<Result> get(Get get) throws CrudException {
    setTargetToIfNot(get);
    get.clearProjections(); // project all
    try {
      return crud.get(get);
    } catch (UncommittedRecordException e) {
      lazyRecovery(get, e.getResults());
      throw e;
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
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
    setTargetToIfNot(put);
    crud.put(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkArgument(puts.size() != 0);
    puts.forEach(p -> put(p));
  }

  @Override
  public void delete(Delete delete) {
    setTargetToIfNot(delete);
    crud.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) {
    checkArgument(deletes.size() != 0);
    deletes.forEach(d -> delete(d));
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

  private void setTargetToIfNot(Operation operation) {
    if (!operation.forNamespace().isPresent()) {
      operation.forNamespace(namespace.orElse(null));
    }
    if (!operation.forTable().isPresent()) {
      operation.forTable(tableName.orElse(null));
    }
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
  }
}
