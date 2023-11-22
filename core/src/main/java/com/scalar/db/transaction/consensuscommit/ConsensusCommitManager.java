package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitter3;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.KeyManipulator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ConsensusCommitManager extends ActiveTransactionManagedDistributedTransactionManager {
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
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;

  ////////////// For group commit >>>>>>>>>>>>>>>>>
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_ENABLED =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_ENABLED";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_THREADS =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_NUM_OF_THREADS";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_SIZE_FIX_EXPIRATION_IN_MILLIS =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_SIZE_FIX_EXPIRATION_IN_MILLIS";

  private final GroupCommitter3<String, Snapshot> groupCommitter;

  // FIXME: GroupCommitter3 should be abstract
  @VisibleForTesting
  public static Optional<GroupCommitter3<String, Snapshot>> prepareGroupCommitter() {
    // TODO: Make this configurable
    // TODO: Take care of lazy recovery
    if (!"true".equalsIgnoreCase(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_ENABLED))) {
      return Optional.empty();
    }

    int groupCommitNumOfRetentionValues = 32;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES) != null) {
      groupCommitNumOfRetentionValues =
          Integer.parseInt(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES));
    }

    int groupCommitNumOfThreads = 32;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_THREADS) != null) {
      groupCommitNumOfThreads =
          Integer.parseInt(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_THREADS));
    }

    int groupCommitSizeFixExpirationInMillis = 200;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_SIZE_FIX_EXPIRATION_IN_MILLIS) != null) {
      groupCommitSizeFixExpirationInMillis =
          Integer.parseInt(
              System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_SIZE_FIX_EXPIRATION_IN_MILLIS));
    }
    return Optional.of(
        new GroupCommitter3<>(
            "coordinator-writer",
            groupCommitSizeFixExpirationInMillis,
            groupCommitNumOfRetentionValues,
            10,
            groupCommitNumOfThreads,
            new KeyManipulator<String>() {
              @Override
              public String createParentKey() {
                return UUID.randomUUID().toString();
              }

              @Override
              public String createFullKey(String parentKey, String childKey) {
                return parentKey + ":" + childKey;
              }

              @Override
              public Keys<String> fromFullKey(String fullKey) {
                String[] parts = fullKey.split(":");
                if (parts.length != 2) {
                  throw new IllegalArgumentException("Invalid full key. key:" + fullKey);
                }
                return new Keys<>(parts[0], parts[1]);
              }
            }));
  }
  ////////////// For group commit <<<<<<<<<<<<<<<<<

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public ConsensusCommitManager(
      DistributedStorage storage, DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    super(databaseConfig);
    this.storage = storage;
    this.admin = admin;
    config = new ConsensusCommitConfig(databaseConfig);
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    groupCommitter = prepareGroupCommitter().orElse(null);
    commit =
        new CommitHandler(
            storage, coordinator, tableMetadataManager, parallelExecutor, groupCommitter);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  ConsensusCommitManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    storage = storageFactory.getStorage();
    admin = storageFactory.getStorageAdmin();

    config = new ConsensusCommitConfig(databaseConfig);
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    groupCommitter = prepareGroupCommitter().orElse(null);
    commit =
        new CommitHandler(
            storage, coordinator, tableMetadataManager, parallelExecutor, groupCommitter);
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  ConsensusCommitManager(
      DistributedStorage storage,
      DistributedStorageAdmin admin,
      ConsensusCommitConfig config,
      DatabaseConfig databaseConfig,
      Coordinator coordinator,
      ParallelExecutor parallelExecutor,
      RecoveryHandler recovery,
      CommitHandler commit,
      GroupCommitter3<String, Snapshot> groupCommitter) {
    super(databaseConfig);
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
    // For PoC
    this.groupCommitter = groupCommitter;
    this.isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    this.mutationOperationChecker =
        new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    return begin(config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    return begin(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(com.scalar.db.api.Isolation isolation)
      throws TransactionException {
    return begin(Isolation.valueOf(isolation.name()), config.getSerializableStrategy());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, com.scalar.db.api.Isolation isolation)
      throws TransactionException {
    return begin(txId, Isolation.valueOf(isolation.name()), config.getSerializableStrategy());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      com.scalar.db.api.Isolation isolation, com.scalar.db.api.SerializableStrategy strategy)
      throws TransactionException {
    return begin(Isolation.valueOf(isolation.name()), (SerializableStrategy) strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(com.scalar.db.api.SerializableStrategy strategy)
      throws TransactionException {
    return begin(Isolation.SERIALIZABLE, (SerializableStrategy) strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, com.scalar.db.api.SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId, Isolation.SERIALIZABLE, (SerializableStrategy) strategy);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId,
      com.scalar.db.api.Isolation isolation,
      com.scalar.db.api.SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId, Isolation.valueOf(isolation.name()), (SerializableStrategy) strategy);
  }

  @VisibleForTesting
  DistributedTransaction begin(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    String txId;
    if (groupCommitter != null) {
      txId = groupCommitter.reserve(UUID.randomUUID().toString());
    } else {
      txId = UUID.randomUUID().toString();
    }
    return begin(txId, isolation, strategy);
  }

  @VisibleForTesting
  DistributedTransaction begin(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkNotNull(isolation);
    if (!config.getIsolation().equals(isolation)
        || !config.getSerializableStrategy().equals(strategy)) {
      logger.warn(
          "Setting different isolation level or serializable strategy from the ones"
              + "in DatabaseConfig might cause unexpected anomalies");
    }
    Snapshot snapshot =
        new Snapshot(txId, isolation, strategy, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(
            storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled, parallelExecutor);
    ConsensusCommit consensus =
        new ConsensusCommit(crud, commit, recovery, mutationOperationChecker, groupCommitter);
    getNamespace().ifPresent(consensus::withNamespace);
    getTable().ifPresent(consensus::withTable);
    return decorate(consensus);
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
      return commit.abortState(txId);
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
