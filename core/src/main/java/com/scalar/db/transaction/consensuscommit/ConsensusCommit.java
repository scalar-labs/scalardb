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
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A transaction manager that implements a transaction protocol on the basis of two-phase commit on
 * the consensus of an underlining storage.
 *
 * <p>When SERIALIZABLE is specified in {@link Isolation}, it makes schedule strict serializable or
 * serializable depending on underlining database operations. If a transaction runs on linearizable
 * operations, it makes it strict serializable. If a transaction runs on serializable operations, it
 * makes it serializable.
 *
 * <p>When SNAPSHOT is specified in {@link Isolation}, it makes it a weaker variant of snapshot
 * isolation (SI). This snapshot isolation could cause read skew anomalies in addition to write skew
 * and read-only anomalies, which are known to be usual SI anomalies.
 */
@NotThreadSafe
public class ConsensusCommit implements DistributedTransaction {
  private final CrudHandler crud;
  private final CommitHandler commit;
  private Optional<String> namespace;
  private Optional<String> tableName;

  private Runnable beforeCommitHook;

  public ConsensusCommit(CrudHandler crud, CommitHandler commit) {
    this.crud = checkNotNull(crud);
    this.commit = checkNotNull(commit);
    namespace = Optional.empty();
    tableName = Optional.empty();
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

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(get, namespace, tableName);
    List<String> projections = new ArrayList<>(get.getProjections());
    return crud.get(get).map(r -> new FilteredResult(r, projections));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(scan, namespace, tableName);
    List<String> projections = new ArrayList<>(scan.getProjections());
    return crud.scan(scan).stream()
        .map(r -> new FilteredResult(r, projections))
        .collect(Collectors.toList());
  }

  @Override
  public void put(Put put) {
    ScalarDbUtils.setTargetToIfNot(put, namespace, tableName);
    crud.put(put);
  }

  @Override
  public void put(List<Put> puts) {
    checkArgument(puts.size() != 0);
    puts.forEach(this::put);
  }

  @Override
  public void delete(Delete delete) {
    ScalarDbUtils.setTargetToIfNot(delete, namespace, tableName);
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
  void setBeforeCommitHook(Runnable beforeCommitHook) {
    this.beforeCommitHook = beforeCommitHook;
  }
}
