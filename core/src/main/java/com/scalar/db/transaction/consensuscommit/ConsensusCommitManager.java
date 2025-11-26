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
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractDistributedTransactionManager;
import com.scalar.db.common.AbstractTransactionManagerCrudOperableScanner;
import com.scalar.db.common.ReadOnlyDistributedTransaction;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.VirtualTableInfoManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ConsensusCommitManager extends AbstractDistributedTransactionManager {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusCommitManager.class);
  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final Coordinator coordinator;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryExecutor recoveryExecutor;
  private final CrudHandler crud;
  protected final CommitHandler commit;
  private final Isolation isolation;
  private final ConsensusCommitOperationChecker operationChecker;
  @Nullable private final CoordinatorGroupCommitter groupCommitter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public ConsensusCommitManager(
      DistributedStorage storage, DistributedStorageAdmin admin, DatabaseConfig databaseConfig) {
    super(databaseConfig);
    this.storage = storage;
    this.admin = admin;
    ConsensusCommitConfig config = new ConsensusCommitConfig(databaseConfig);
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    RecoveryHandler recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    recoveryExecutor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);
    groupCommitter = CoordinatorGroupCommitter.from(config).orElse(null);
    crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            config.isIncludeMetadataEnabled(),
            parallelExecutor);
    StorageInfoProvider storageInfoProvider = new StorageInfoProvider(admin);
    commit = createCommitHandler(config, storageInfoProvider);
    isolation = config.getIsolation();
    VirtualTableInfoManager virtualTableInfoManager =
        new VirtualTableInfoManager(admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    operationChecker =
        new ConsensusCommitOperationChecker(
            tableMetadataManager,
            virtualTableInfoManager,
            storageInfoProvider,
            config.isIncludeMetadataEnabled());
  }

  protected ConsensusCommitManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    storage = storageFactory.getStorage();
    admin = storageFactory.getStorageAdmin();

    ConsensusCommitConfig config = new ConsensusCommitConfig(databaseConfig);
    coordinator = new Coordinator(storage, config);
    parallelExecutor = new ParallelExecutor(config);
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    RecoveryHandler recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    recoveryExecutor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);
    groupCommitter = CoordinatorGroupCommitter.from(config).orElse(null);
    crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            config.isIncludeMetadataEnabled(),
            parallelExecutor);
    StorageInfoProvider storageInfoProvider = new StorageInfoProvider(admin);
    commit = createCommitHandler(config, storageInfoProvider);
    isolation = config.getIsolation();
    VirtualTableInfoManager virtualTableInfoManager =
        new VirtualTableInfoManager(admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    operationChecker =
        new ConsensusCommitOperationChecker(
            tableMetadataManager,
            virtualTableInfoManager,
            storageInfoProvider,
            config.isIncludeMetadataEnabled());
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
      RecoveryExecutor recoveryExecutor,
      CrudHandler crud,
      CommitHandler commit,
      Isolation isolation,
      @Nullable CoordinatorGroupCommitter groupCommitter) {
    super(databaseConfig);
    this.storage = storage;
    this.admin = admin;
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    this.coordinator = coordinator;
    this.parallelExecutor = parallelExecutor;
    this.recoveryExecutor = recoveryExecutor;
    this.crud = crud;
    this.commit = commit;
    this.groupCommitter = groupCommitter;
    this.isolation = isolation;
    VirtualTableInfoManager virtualTableInfoManager =
        new VirtualTableInfoManager(admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    StorageInfoProvider storageInfoProvider = new StorageInfoProvider(admin);
    this.operationChecker =
        new ConsensusCommitOperationChecker(
            tableMetadataManager,
            virtualTableInfoManager,
            storageInfoProvider,
            config.isIncludeMetadataEnabled());
  }

  // `groupCommitter` must be set before calling this method.
  private CommitHandler createCommitHandler(
      ConsensusCommitConfig config, StorageInfoProvider storageInfoProvider) {
    MutationsGrouper mutationsGrouper = new MutationsGrouper(storageInfoProvider);
    if (isGroupCommitEnabled()) {
      return new CommitHandlerWithGroupCommit(
          storage,
          coordinator,
          tableMetadataManager,
          parallelExecutor,
          mutationsGrouper,
          config.isCoordinatorWriteOmissionOnReadOnlyEnabled(),
          config.isOnePhaseCommitEnabled(),
          groupCommitter);
    } else {
      return new CommitHandler(
          storage,
          coordinator,
          tableMetadataManager,
          parallelExecutor,
          mutationsGrouper,
          config.isCoordinatorWriteOmissionOnReadOnlyEnabled(),
          config.isOnePhaseCommitEnabled());
    }
  }

  @Override
  public DistributedTransaction begin() {
    String txId = UUID.randomUUID().toString();
    return begin(txId);
  }

  @Override
  public DistributedTransaction begin(String txId) {
    return begin(txId, isolation, false, false);
  }

  @Override
  public DistributedTransaction beginReadOnly() {
    String txId = UUID.randomUUID().toString();
    return beginReadOnly(txId);
  }

  @Override
  public DistributedTransaction beginReadOnly(String txId) {
    return begin(txId, isolation, true, false);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(com.scalar.db.api.Isolation isolation) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, Isolation.valueOf(isolation.name()), false, false);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, com.scalar.db.api.Isolation isolation) {
    return begin(txId, Isolation.valueOf(isolation.name()), false, false);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      com.scalar.db.api.Isolation isolation, com.scalar.db.api.SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, Isolation.valueOf(isolation.name()), false, false);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(com.scalar.db.api.SerializableStrategy strategy) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, Isolation.SERIALIZABLE, false, false);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, com.scalar.db.api.SerializableStrategy strategy) {
    return begin(txId, Isolation.SERIALIZABLE, false, false);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId,
      com.scalar.db.api.Isolation isolation,
      com.scalar.db.api.SerializableStrategy strategy) {
    return begin(txId, Isolation.valueOf(isolation.name()), false, false);
  }

  @VisibleForTesting
  DistributedTransaction begin(
      String txId, Isolation isolation, boolean readOnly, boolean oneOperation) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkNotNull(isolation);
    if (!readOnly && isGroupCommitEnabled()) {
      assert groupCommitter != null;
      txId = groupCommitter.reserve(txId);
    }
    if (!this.isolation.equals(isolation)) {
      logger.warn(
          "Setting different isolation level from the one in DatabaseConfig might cause unexpected "
              + "anomalies");
    }
    Snapshot snapshot = new Snapshot(txId, tableMetadataManager);
    TransactionContext context =
        new TransactionContext(txId, snapshot, isolation, readOnly, oneOperation);
    DistributedTransaction transaction =
        new ConsensusCommit(context, crud, commit, operationChecker, groupCommitter);
    if (readOnly) {
      transaction = new ReadOnlyDistributedTransaction(transaction);
    }
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return transaction;
  }

  private DistributedTransaction beginOneOperation(boolean readOnly) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation, readOnly, true);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)), true);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.scan(copyAndSetTargetToIfNot(scan)), true);
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    DistributedTransaction transaction = beginOneOperation(true);

    TransactionCrudOperable.Scanner scanner;
    try {
      scanner = transaction.getScanner(copyAndSetTargetToIfNot(scan));
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    }

    return new AbstractTransactionManagerCrudOperableScanner() {

      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public Optional<Result> one() throws CrudException {
        try {
          return scanner.one();
        } catch (CrudException e) {
          closed.set(true);

          try {
            scanner.close();
          } catch (CrudException ex) {
            e.addSuppressed(ex);
          }

          rollbackTransaction(transaction);
          throw e;
        }
      }

      @Override
      public List<Result> all() throws CrudException {
        try {
          return scanner.all();
        } catch (CrudException e) {
          closed.set(true);

          try {
            scanner.close();
          } catch (CrudException ex) {
            e.addSuppressed(ex);
          }

          rollbackTransaction(transaction);
          throw e;
        }
      }

      @Override
      public void close() throws CrudException, UnknownTransactionStatusException {
        if (closed.get()) {
          return;
        }
        closed.set(true);

        try {
          scanner.close();
        } catch (CrudException e) {
          rollbackTransaction(transaction);
          throw e;
        }

        try {
          transaction.commit();
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
    };
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(put));
          return null;
        },
        false);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(puts));
          return null;
        },
        false);
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.insert(copyAndSetTargetToIfNot(insert));
          return null;
        },
        false);
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.upsert(copyAndSetTargetToIfNot(upsert));
          return null;
        },
        false);
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.update(copyAndSetTargetToIfNot(update));
          return null;
        },
        false);
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(delete));
          return null;
        },
        false);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(deletes));
          return null;
        },
        false);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.mutate(copyAndSetTargetToIfNot(mutations));
          return null;
        },
        false);
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations)
      throws CrudException, UnknownTransactionStatusException {
    boolean readOnly = operations.stream().allMatch(o -> o instanceof Selection);
    return executeTransaction(t -> t.batch(operations), readOnly);
  }

  private <R> R executeTransaction(
      ThrowableFunction<DistributedTransaction, R, TransactionException> throwableFunction,
      boolean readOnly)
      throws CrudException, UnknownTransactionStatusException {
    DistributedTransaction transaction = beginOneOperation(readOnly);

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

  private void rollbackTransaction(DistributedTransaction transaction) {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      logger.warn(
          "Rolling back the transaction failed. Transaction ID: {}", transaction.getId(), e);
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
    recoveryExecutor.close();
    if (isGroupCommitEnabled()) {
      assert groupCommitter != null;
      groupCommitter.close();
    }
  }
}
