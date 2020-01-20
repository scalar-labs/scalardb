package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CoordinatorException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitManager implements DistributedTransactionManager {
  private final DistributedStorage storage;
  private Coordinator coordinator;
  private RecoveryHandler recovery;
  private CommitHandler commit;
  private String namespace;
  private String tableName;

  @Inject
  public ConsensusCommitManager(DistributedStorage storage) {
    this.storage = storage;
    this.coordinator = new Coordinator(storage);
    this.recovery = new RecoveryHandler(storage, coordinator);
    this.commit = new CommitHandler(storage, coordinator, recovery);
  }

  @VisibleForTesting
  ConsensusCommitManager(
      DistributedStorage storage,
      Coordinator coordinator,
      RecoveryHandler recovery,
      CommitHandler commit) {
    this.storage = storage;
    this.coordinator = coordinator;
    this.recovery = recovery;
    this.commit = commit;
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = namespace;
    this.tableName = tableName;
  }

  @Override
  public ConsensusCommit start() {
    return start(Isolation.SNAPSHOT);
  }

  @Override
  public ConsensusCommit start(String txId) {
    return start(txId, Isolation.SNAPSHOT);
  }

  @Override
  public synchronized ConsensusCommit start(Isolation isolation) {
    String txId = UUID.randomUUID().toString();
    return start(txId, isolation);
  }

  @Override
  public synchronized ConsensusCommit start(String txId, Isolation isolation) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkArgument(isolation != null);
    Snapshot snapshot = new Snapshot(txId, isolation);
    CrudHandler crud = new CrudHandler(storage, snapshot);
    ConsensusCommit consensus = new ConsensusCommit(crud, commit, recovery);
    consensus.with(namespace, tableName);
    return consensus;
  }

  @Override
  public TransactionState getState(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    try {
      Optional<State> state = coordinator.getState(txId);
      if (state.isPresent()) {
        return state.get().getState();
      }
    } catch (CoordinatorException e) {
      // ignore
    }
    // Either no state exists or the exception is thrown
    return TransactionState.UNKNOWN;
  }

  @Override
  public void close() {
    storage.close();
  }
}
