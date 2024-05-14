package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class TwoPhaseConsensusCommitManager
    extends ActiveTransactionManagedTwoPhaseCommitTransactionManager {

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
  private final CoordinatorGroupCommitter groupCommitter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public TwoPhaseConsensusCommitManager(
      DistributedStorage storage, DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    super(databaseConfig);
    this.storage = storage;
    this.admin = admin;
    config = new ConsensusCommitConfig(databaseConfig);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    groupCommitter = CoordinatorGroupCommitter.from(config).orElse(null);
    commit = createCommitHandler();
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  public TwoPhaseConsensusCommitManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    storage = storageFactory.getStorage();
    admin = storageFactory.getStorageAdmin();

    config = new ConsensusCommitConfig(databaseConfig);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    groupCommitter = CoordinatorGroupCommitter.from(config).orElse(null);
    commit = createCommitHandler();
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  TwoPhaseConsensusCommitManager(
      DistributedStorage storage,
      DistributedStorageAdmin admin,
      ConsensusCommitConfig config,
      DatabaseConfig databaseConfig,
      Coordinator coordinator,
      ParallelExecutor parallelExecutor,
      RecoveryHandler recovery,
      CommitHandler commit,
      CoordinatorGroupCommitter groupCommitter) {
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
    this.groupCommitter = groupCommitter;
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  // `groupCommitter` must be set before calling this method.
  private CommitHandler createCommitHandler() {
    if (isGroupCommitEnabled()) {
      return new CommitHandlerWithGroupCommit(
          storage, coordinator, tableMetadataManager, parallelExecutor, groupCommitter);
    } else {
      return new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
    }
  }

  @Override
  public TwoPhaseCommitTransaction begin() throws TransactionException {
    String txId = UUID.randomUUID().toString();
    return begin(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public TwoPhaseCommitTransaction begin(String txId) throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return begin(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction begin(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation, strategy);
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction begin(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    if (isGroupCommitEnabled()) {
      txId = groupCommitter.reserve(txId);
    }
    return createNewTransaction(txId, isolation, strategy, true);
  }

  // The group commit feature must not be used in a participant even if it's enabled in order to
  // avoid writing different coordinator record images for the same transaction, which depends on
  // group commit timing and the progresses of other transactions in the same group.
  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return join(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction join(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    // If the transaction associated with the specified transaction ID is active, resume it
    if (isTransactionActive(txId)) {
      return resume(txId);
    }

    return createNewTransaction(txId, isolation, strategy, false);
  }

  private TwoPhaseCommitTransaction createNewTransaction(
      String txId, Isolation isolation, SerializableStrategy strategy, boolean isCoordinator)
      throws TransactionException {
    Snapshot snapshot =
        new Snapshot(txId, isolation, strategy, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(
            storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled, parallelExecutor);

    // If the group commit feature is enabled, only the coordinator service must manage the
    // coordinator table state of transactions. With the group commit feature enabled, transactions
    // are grouped and managed in memory on a node based on various events (e.g., timeouts).  It's
    // highly likely that the coordinator and participants in the two-phase commit interface will
    // group and manage transactions differently, resulting in attempts to store different
    // transaction groups in the coordinator table. Therefore, TwoPhaseConsensusCommit must not
    // commit or abort states if it's a participant when the group commit feature is enabled.
    boolean shouldManageCoordinatorState = isCoordinator || !isGroupCommitEnabled();
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(
            crud, commit, recovery, mutationOperationChecker, shouldManageCoordinatorState);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return decorate(transaction);
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

  private boolean isGroupCommitEnabled() {
    return groupCommitter != null;
  }

  @Override
  public void close() {
    storage.close();
    admin.close();
    parallelExecutor.close();
    if (isGroupCommitEnabled()) {
      groupCommitter.close();
    }
  }
}
