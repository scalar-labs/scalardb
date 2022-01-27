package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ConsensusCommitManager implements DistributedTransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusCommitManager.class);
  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final ConsensusCommitConfig config;
  private final TransactionalTableMetadataManager tableMetadataManager;
  private final Coordinator coordinator;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryHandler recovery;
  private final CommitHandler commit;
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public ConsensusCommitManager(
      DistributedStorage storage, DistributedStorageAdmin admin, ConsensusCommitConfig config) {
    this.storage = storage;
    this.admin = admin;
    this.config = config;
    tableMetadataManager =
        new TransactionalTableMetadataManager(
            admin, config.getTableMetadataCacheExpirationTimeSecs());
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
    commit = new CommitHandler(storage, coordinator, recovery, parallelExecutor);
    namespace = storage.getNamespace();
    tableName = storage.getTable();
  }

  @VisibleForTesting
  public ConsensusCommitManager(
      DistributedStorage storage,
      DistributedStorageAdmin admin,
      ConsensusCommitConfig config,
      Coordinator coordinator,
      ParallelExecutor parallelExecutor,
      RecoveryHandler recovery,
      CommitHandler commit) {
    this.storage = storage;
    this.admin = admin;
    this.config = config;
    tableMetadataManager =
        new TransactionalTableMetadataManager(
            admin, config.getTableMetadataCacheExpirationTimeSecs());
    this.coordinator = coordinator;
    this.parallelExecutor = parallelExecutor;
    this.recovery = recovery;
    this.commit = commit;
    namespace = storage.getNamespace();
    tableName = storage.getTable();
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
    return start(config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public ConsensusCommit start(String txId) {
    return start(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @Deprecated
  @Override
  public ConsensusCommit start(com.scalar.db.api.Isolation isolation) {
    return start(Isolation.valueOf(isolation.name()), config.getSerializableStrategy());
  }

  @Deprecated
  @Override
  public ConsensusCommit start(String txId, com.scalar.db.api.Isolation isolation) {
    return start(txId, Isolation.valueOf(isolation.name()), config.getSerializableStrategy());
  }

  @Deprecated
  @Override
  public ConsensusCommit start(
      com.scalar.db.api.Isolation isolation, com.scalar.db.api.SerializableStrategy strategy) {
    return start(Isolation.valueOf(isolation.name()), (SerializableStrategy) strategy);
  }

  @Deprecated
  @Override
  public ConsensusCommit start(com.scalar.db.api.SerializableStrategy strategy) {
    return start(Isolation.SERIALIZABLE, (SerializableStrategy) strategy);
  }

  @Deprecated
  @Override
  public ConsensusCommit start(String txId, com.scalar.db.api.SerializableStrategy strategy) {
    return start(txId, Isolation.SERIALIZABLE, (SerializableStrategy) strategy);
  }

  @Deprecated
  @Override
  public ConsensusCommit start(
      String txId,
      com.scalar.db.api.Isolation isolation,
      com.scalar.db.api.SerializableStrategy strategy) {
    return start(txId, Isolation.valueOf(isolation.name()), (SerializableStrategy) strategy);
  }

  @VisibleForTesting
  ConsensusCommit start(Isolation isolation, SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return start(txId, isolation, strategy);
  }

  @VisibleForTesting
  ConsensusCommit start(String txId, Isolation isolation, SerializableStrategy strategy) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkNotNull(isolation);
    if (!config.getIsolation().equals(isolation)
        || !config.getSerializableStrategy().equals(strategy)) {
      LOGGER.warn(
          "Setting different isolation level or serializable strategy from the ones"
              + "in DatabaseConfig might cause unexpected anomalies.");
    }
    Snapshot snapshot = new Snapshot(txId, isolation, strategy, parallelExecutor);
    CrudHandler crud = new CrudHandler(storage, snapshot, recovery, tableMetadataManager);
    ConsensusCommit consensus = new ConsensusCommit(crud, commit);
    namespace.ifPresent(consensus::withNamespace);
    tableName.ifPresent(consensus::withTable);
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
    } catch (CoordinatorException ignored) {
      // ignored
    }
    // Either no state exists or the exception is thrown
    return TransactionState.UNKNOWN;
  }

  @Override
  public TransactionState abort(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    try {
      return commit.abort(txId);
    } catch (UnknownTransactionStatusException ignored) {
      return TransactionState.UNKNOWN;
    }
  }

  @Override
  public void close() {
    storage.close();
    admin.close();
    parallelExecutor.close();
  }
}
