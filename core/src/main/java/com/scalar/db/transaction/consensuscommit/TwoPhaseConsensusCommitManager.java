package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.common.AbstractTwoPhaseCommitTransactionManager;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.ActiveExpiringMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TwoPhaseConsensusCommitManager extends AbstractTwoPhaseCommitTransactionManager {
  private static final Logger logger =
      LoggerFactory.getLogger(TwoPhaseConsensusCommitManager.class);

  private static final long TRANSACTION_LIFETIME_MILLIS = 60000;
  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final ConsensusCommitConfig config;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final Coordinator coordinator;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryHandler recovery;
  private final CommitHandler commit;
  private final ActiveExpiringMap<String, TwoPhaseConsensusCommit> activeTransactions;
  private final boolean isIncludeMetadataEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public TwoPhaseConsensusCommitManager(
      DistributedStorage storage, DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    this.storage = storage;
    this.admin = admin;
    config = new ConsensusCommitConfig(databaseConfig);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    commit = new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
    activeTransactions =
        new ActiveExpiringMap<>(
            TRANSACTION_LIFETIME_MILLIS,
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            t -> {
              logger.warn("the transaction is expired. transactionId: {}", t.getId());
              try {
                t.rollback();
              } catch (RollbackException e) {
                logger.warn("rollback failed", e);
              }
            });
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  public TwoPhaseConsensusCommitManager(DatabaseConfig databaseConfig) {
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
    commit = new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);

    activeTransactions =
        new ActiveExpiringMap<>(
            TRANSACTION_LIFETIME_MILLIS,
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            t -> {
              logger.warn("the transaction is expired. transactionId: {}", t.getId());
              try {
                t.rollback();
              } catch (RollbackException e) {
                logger.warn("rollback failed", e);
              }
            });
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
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
    activeTransactions = new ActiveExpiringMap<>(Long.MAX_VALUE, Long.MAX_VALUE, t -> {});
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
  }

  @Override
  public TwoPhaseConsensusCommit begin() {
    String txId = UUID.randomUUID().toString();
    return begin(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public TwoPhaseConsensusCommit begin(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return begin(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public TwoPhaseConsensusCommit start() throws TransactionException {
    return (TwoPhaseConsensusCommit) super.start();
  }

  @Override
  public TwoPhaseConsensusCommit start(String txId) throws TransactionException {
    return (TwoPhaseConsensusCommit) super.start(txId);
  }

  @VisibleForTesting
  TwoPhaseConsensusCommit begin(Isolation isolation, SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation, strategy);
  }

  @VisibleForTesting
  TwoPhaseConsensusCommit begin(String txId, Isolation isolation, SerializableStrategy strategy) {
    return createNewTransaction(txId, true, isolation, strategy);
  }

  @Override
  public TwoPhaseConsensusCommit join(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return join(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @VisibleForTesting
  TwoPhaseConsensusCommit join(String txId, Isolation isolation, SerializableStrategy strategy) {
    return createNewTransaction(txId, false, isolation, strategy);
  }

  private TwoPhaseConsensusCommit createNewTransaction(
      String txId, boolean isCoordinator, Isolation isolation, SerializableStrategy strategy) {
    Snapshot snapshot =
        new Snapshot(txId, isolation, strategy, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled);

    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator);

    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return transaction;
  }

  @Override
  public void suspend(TwoPhaseCommitTransaction transaction) throws TransactionException {
    if (activeTransactions.putIfAbsent(transaction.getId(), (TwoPhaseConsensusCommit) transaction)
        != null) {
      transaction.rollback();
      throw new TransactionException("The transaction already exists");
    }
  }

  @Override
  public TwoPhaseConsensusCommit resume(String txId) throws TransactionException {
    TwoPhaseConsensusCommit transaction = activeTransactions.remove(txId);
    if (transaction == null) {
      throw new TransactionException(
          "A transaction associated with the specified transaction ID is not found. "
              + "It might have been expired");
    }
    return transaction;
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
