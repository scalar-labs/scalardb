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
import com.scalar.db.util.groupcommit.GroupCommitter;
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

  ////////////// For group commit >>>>>>>>>>>>>>>>>
  private final GroupCommitter<String, Snapshot> groupCommitter;

  public boolean isGroupCommitEnabled() {
    return groupCommitter != null;
  }
  ////////////// For group commit <<<<<<<<<<<<<<<<<

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
    groupCommitter = ConsensusCommitUtils.prepareGroupCommitter().orElse(null);
    commit =
        new CommitHandler(
            storage, coordinator, tableMetadataManager, parallelExecutor, groupCommitter);
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
    groupCommitter = ConsensusCommitUtils.prepareGroupCommitter().orElse(null);
    commit =
        new CommitHandler(
            storage, coordinator, tableMetadataManager, parallelExecutor, groupCommitter);
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
      GroupCommitter<String, Snapshot> groupCommitter) {
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
    // For Group Commit PoC >>>>
    if (groupCommitter != null) {
      txId = groupCommitter.reserve(txId);
    }
    // <<<< For Group Commit PoC
    return createNewTransaction(txId, isolation, strategy);
  }

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

    return createNewTransaction(txId, isolation, strategy);
  }

  private TwoPhaseCommitTransaction createNewTransaction(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    Snapshot snapshot =
        new Snapshot(txId, isolation, strategy, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(
            storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled, parallelExecutor);

    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(
            crud, commit, recovery, mutationOperationChecker, groupCommitter);
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

  @Override
  public void close() {
    storage.close();
    admin.close();
    parallelExecutor.close();
    // For Group Commit PoC >>>>
    if (groupCommitter != null) {
      groupCommitter.close();
    }
    // <<<< For Group Commit PoC
  }
}
