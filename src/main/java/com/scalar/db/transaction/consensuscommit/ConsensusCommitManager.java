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
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public ConsensusCommitManager(DistributedStorage storage) {
    this.storage = storage;
    this.coordinator = new Coordinator(storage);
    this.recovery = new RecoveryHandler(storage, coordinator);
    this.commit = new CommitHandler(storage, coordinator, recovery);
    this.namespace = storage.getNamespace();
    this.tableName = storage.getTable();
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
    this.namespace = storage.getNamespace();
    this.tableName = storage.getTable();
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
  public ConsensusCommit start() {
    return start(Isolation.SNAPSHOT);
  }

  @Override
  public ConsensusCommit start(String txId) {
    return start(txId, Isolation.SNAPSHOT);
  }

  @Override
  public synchronized ConsensusCommit start(Isolation isolation) {
    return start(isolation, SerializableStrategy.EXTRA_WRITE);
  }

  @Override
  public synchronized ConsensusCommit start(String txId, Isolation isolation) {
    return start(txId, isolation, SerializableStrategy.EXTRA_WRITE);
  }

  @Override
  public synchronized ConsensusCommit start(
      Isolation isolation, com.scalar.db.api.SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return start(txId, isolation, strategy);
  }

  @Override
  public synchronized ConsensusCommit start(com.scalar.db.api.SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return start(txId, Isolation.SERIALIZABLE, strategy);
  }

  @Override
  public synchronized ConsensusCommit start(
      String txId, com.scalar.db.api.SerializableStrategy strategy) {
    return start(txId, Isolation.SERIALIZABLE, strategy);
  }

  @Override
  public synchronized ConsensusCommit start(
      String txId, Isolation isolation, com.scalar.db.api.SerializableStrategy strategy) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkArgument(isolation != null);
    Snapshot snapshot = new Snapshot(txId, isolation, (SerializableStrategy) strategy);
    CrudHandler crud = new CrudHandler(storage, snapshot);
    ConsensusCommit consensus = new ConsensusCommit(crud, commit, recovery);
    namespace.ifPresent(n -> consensus.withNamespace(n));
    tableName.ifPresent(t -> consensus.withTable(t));
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
