package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.common.AbstractDistributedTransactionManager;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ConsensusCommitManager extends AbstractDistributedTransactionManager {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusCommitManager.class);
  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final ConsensusCommitConfig config;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final Coordinator coordinator;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryHandler recovery;
  private final CommitHandler commit;
  private final boolean isIncludeMetadataEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public ConsensusCommitManager(
      DistributedStorage storage, DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    this.storage = storage;
    this.admin = admin;
    config = new ConsensusCommitConfig(databaseConfig);
    this.coordinator = new Coordinator(storage, config);
    this.parallelExecutor = new ParallelExecutor(config);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    commit = new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  public ConsensusCommitManager(
      DistributedStorage storage,
      DistributedStorageAdmin admin,
      ConsensusCommitConfig config,
      DatabaseConfig databaseConfig,
      Coordinator coordinator,
      ParallelExecutor parallelExecutor,
      RecoveryHandler recovery,
      CommitHandler commit) {
    this.storage = storage;
    this.admin = admin;
    this.config = config;
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    this.coordinator = coordinator;
    this.parallelExecutor = parallelExecutor;
    this.recovery = recovery;
    this.commit = commit;
    this.isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  @Override
  public ConsensusCommit begin() {
    return begin(config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public ConsensusCommit begin(String txId) {
    return begin(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public ConsensusCommit start() throws TransactionException {
    return (ConsensusCommit) super.start();
  }

  @Override
  public ConsensusCommit start(String txId) throws TransactionException {
    return (ConsensusCommit) super.start(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public ConsensusCommit start(com.scalar.db.api.Isolation isolation) {
    return begin(Isolation.valueOf(isolation.name()), config.getSerializableStrategy());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public ConsensusCommit start(String txId, com.scalar.db.api.Isolation isolation) {
    return begin(txId, Isolation.valueOf(isolation.name()), config.getSerializableStrategy());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public ConsensusCommit start(
      com.scalar.db.api.Isolation isolation, com.scalar.db.api.SerializableStrategy strategy) {
    return begin(Isolation.valueOf(isolation.name()), (SerializableStrategy) strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public ConsensusCommit start(com.scalar.db.api.SerializableStrategy strategy) {
    return begin(Isolation.SERIALIZABLE, (SerializableStrategy) strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public ConsensusCommit start(String txId, com.scalar.db.api.SerializableStrategy strategy) {
    return begin(txId, Isolation.SERIALIZABLE, (SerializableStrategy) strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public ConsensusCommit start(
      String txId,
      com.scalar.db.api.Isolation isolation,
      com.scalar.db.api.SerializableStrategy strategy) {
    return begin(txId, Isolation.valueOf(isolation.name()), (SerializableStrategy) strategy);
  }

  @VisibleForTesting
  ConsensusCommit begin(Isolation isolation, SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation, strategy);
  }

  @VisibleForTesting
  ConsensusCommit begin(String txId, Isolation isolation, SerializableStrategy strategy) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkNotNull(isolation);
    if (!config.getIsolation().equals(isolation)
        || !config.getSerializableStrategy().equals(strategy)) {
      logger.warn(
          "Setting different isolation level or serializable strategy from the ones"
              + "in DatabaseConfig might cause unexpected anomalies.");
    }
    Snapshot snapshot =
        new Snapshot(txId, isolation, strategy, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled);
    ConsensusCommit consensus = new ConsensusCommit(crud, commit, recovery);
    getNamespace().ifPresent(consensus::withNamespace);
    getTable().ifPresent(consensus::withTable);
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
  public TransactionState rollback(String txId) {
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
