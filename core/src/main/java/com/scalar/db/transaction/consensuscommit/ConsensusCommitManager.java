package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
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
  protected final CommitHandler commit;
  private final boolean isIncludeMetadataEnabled;
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;
  @Nullable private final CoordinatorGroupCommitter groupCommitter;

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
    groupCommitter = CoordinatorGroupCommitter.from(config).orElse(null);
    commit = createCommitHandler();
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  public ConsensusCommitManager(DatabaseConfig databaseConfig) {
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
    groupCommitter = CoordinatorGroupCommitter.from(config).orElse(null);
    commit = createCommitHandler();
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
    this.isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    this.mutationOperationChecker =
        new ConsensusCommitMutationOperationChecker(tableMetadataManager);
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
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation, strategy);
  }

  @VisibleForTesting
  DistributedTransaction begin(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId, isolation, strategy, true);
  }

  private DistributedTransaction begin(
      String txId, Isolation isolation, SerializableStrategy strategy, boolean decorate)
      throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkNotNull(isolation);
    if (isGroupCommitEnabled()) {
      txId = groupCommitter.reserve(txId);
    }
    if (!config.getIsolation().equals(isolation)
        || !config.getSerializableStrategy().equals(strategy)) {
      logger.warn(
          "Setting different isolation level or serializable strategy from the ones "
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
    return decorate ? decorate(consensus) : consensus;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.scan(copyAndSetTargetToIfNot(scan)));
  }

  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(put));
          return null;
        });
  }

  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(puts));
          return null;
        });
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.insert(copyAndSetTargetToIfNot(insert));
          return null;
        });
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.upsert(copyAndSetTargetToIfNot(upsert));
          return null;
        });
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.update(copyAndSetTargetToIfNot(update));
          return null;
        });
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(delete));
          return null;
        });
  }

  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(deletes));
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.mutate(copyAndSetTargetToIfNot(mutations));
          return null;
        });
  }

  private <R> R executeTransaction(
      ThrowableFunction<DistributedTransaction, R, TransactionException> throwableFunction)
      throws CrudException, UnknownTransactionStatusException {
    DistributedTransaction transaction;
    try {
      transaction = beginInternal();
    } catch (TransactionNotFoundException e) {
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (TransactionException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

    try {
      R result = throwableFunction.apply(transaction);
      transaction.commit();
      return result;
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    } catch (CommitConflictException e) {
      rollbackTransaction(transaction);
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (UnknownTransactionStatusException e) {
      throw e;
    } catch (TransactionException e) {
      rollbackTransaction(transaction);
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  @VisibleForTesting
  DistributedTransaction beginInternal() throws TransactionException {
    String txId = UUID.randomUUID().toString();
    return begin(txId, config.getIsolation(), config.getSerializableStrategy(), false);
  }

  private void rollbackTransaction(DistributedTransaction transaction) {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      logger.warn("Rolling back the transaction failed", e);
    }
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

  @VisibleForTesting
  boolean isGroupCommitEnabled() {
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
