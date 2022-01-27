package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.ActiveExpiringMap;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TwoPhaseConsensusCommitManager implements TwoPhaseCommitTransactionManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TwoPhaseConsensusCommitManager.class);

  private static final long TRANSACTION_LIFETIME_MILLIS = 60000;
  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final ConsensusCommitConfig config;
  private final TransactionalTableMetadataManager tableMetadataManager;
  private final Coordinator coordinator;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryHandler recovery;
  private final CommitHandler commit;

  private Optional<String> namespace = Optional.empty();
  private Optional<String> tableName = Optional.empty();

  @Nullable private final ActiveExpiringMap<String, TwoPhaseConsensusCommit> activeTransactions;

  @Inject
  public TwoPhaseConsensusCommitManager(
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

    if (config.isActiveTransactionsManagementEnabled()) {
      activeTransactions =
          new ActiveExpiringMap<>(
              TRANSACTION_LIFETIME_MILLIS,
              TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
              t -> {
                LOGGER.warn("the transaction is expired. transactionId: " + t.getId());
                try {
                  t.rollback();
                } catch (RollbackException e) {
                  LOGGER.warn("rollback failed", e);
                }
              });
    } else {
      activeTransactions = null;
    }
  }

  @VisibleForTesting
  TwoPhaseConsensusCommitManager(
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
    if (config.isActiveTransactionsManagementEnabled()) {
      activeTransactions = new ActiveExpiringMap<>(Long.MAX_VALUE, Long.MAX_VALUE, t -> {});
    } else {
      activeTransactions = null;
    }
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
  public TwoPhaseConsensusCommit start() {
    String txId = UUID.randomUUID().toString();
    return start(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @Override
  public TwoPhaseConsensusCommit start(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return start(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @VisibleForTesting
  TwoPhaseConsensusCommit start(Isolation isolation, SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return start(txId, isolation, strategy);
  }

  @VisibleForTesting
  TwoPhaseConsensusCommit start(String txId, Isolation isolation, SerializableStrategy strategy) {
    return createNewTransaction(txId, true, isolation, strategy);
  }

  @Override
  public TwoPhaseConsensusCommit join(String txId) throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return join(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @VisibleForTesting
  TwoPhaseConsensusCommit join(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    TwoPhaseConsensusCommit transaction = createNewTransaction(txId, false, isolation, strategy);
    if (activeTransactions != null) {
      if (activeTransactions.putIfAbsent(txId, transaction) != null) {
        transaction.rollback();
        throw new TransactionException(
            "The transaction associated with the specified transaction ID already exists");
      }
    }
    return transaction;
  }

  private TwoPhaseConsensusCommit createNewTransaction(
      String txId, boolean isCoordinator, Isolation isolation, SerializableStrategy strategy) {
    Snapshot snapshot = new Snapshot(txId, isolation, strategy, parallelExecutor);
    CrudHandler crud = new CrudHandler(storage, snapshot, recovery, tableMetadataManager);

    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, isCoordinator, this);

    namespace.ifPresent(transaction::withNamespace);
    tableName.ifPresent(transaction::withTable);
    return transaction;
  }

  @Override
  public TwoPhaseConsensusCommit resume(String txId) throws TransactionException {
    if (activeTransactions == null) {
      throw new UnsupportedOperationException(
          "unsupported when setting \""
              + ConsensusCommitConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED
              + "\" to false");
    }

    return activeTransactions
        .get(txId)
        .orElseThrow(
            () ->
                new TransactionException(
                    "A transaction associated with the specified transaction ID is not found. "
                        + "It might have been expired"));
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

  void removeTransaction(String txId) {
    if (activeTransactions == null) {
      return;
    }
    activeTransactions.remove(txId);
  }

  void updateTransactionExpirationTime(String txId) {
    if (activeTransactions == null) {
      return;
    }
    activeTransactions.updateExpirationTime(txId);
  }
}
