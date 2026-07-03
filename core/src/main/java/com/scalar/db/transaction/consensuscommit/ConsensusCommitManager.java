package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
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
import com.scalar.db.common.CoreError;
import com.scalar.db.common.ReadOnlyDistributedTransaction;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.VirtualTableInfoManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private static final CoordinatorGroupCommitKeyManipulator KEY_MANIPULATOR =
      new CoordinatorGroupCommitKeyManipulator();
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
  private final boolean coordinatorWriteOmissionOnReadOnlyEnabled;
  // CBRL: the daemon-fed cache of open backup windows. begin() reads it to capture each
  // transaction's backup label once, at transaction start. Null only in the test-only constructor,
  // where backup mode is not exercised.
  @Nullable private final BackupModeDaemon backupModeDaemon;
  // The oldest cache age begin() accepts before forcing a synchronous scan, and the
  // transaction-lifetime bound stamped into each transaction's context.
  private final long backupStalenessBoundMillis;
  private final long transactionTimeoutMillis;
  // The operator/process id recorded on backup-table transitions for observability.
  private final String backupUpdatedBy = ManagementFactory.getRuntimeMXBean().getName();
  // The label of the backup window this manager opened, so disableRedoLogging() (which takes no
  // label) knows which window to close. Null when no window is open locally.
  @Nullable private volatile String openBackupLabel;

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
    coordinatorWriteOmissionOnReadOnlyEnabled =
        config.isCoordinatorWriteOmissionOnReadOnlyEnabled();
    crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            config.isIncludeMetadataEnabled(),
            config.isIndexEventuallyConsistentReadEnabled(),
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

    ConsensusCommitUtils.warnIfBeforeIndexesAreMissing(admin, config);

    backupStalenessBoundMillis = config.getBackupStalenessBoundMillis();
    transactionTimeoutMillis = config.getTransactionTimeoutMillis();
    backupModeDaemon = new BackupModeDaemon(coordinator, config.getBackupCheckIntervalMillis());
    backupModeDaemon.start();
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
    coordinatorWriteOmissionOnReadOnlyEnabled =
        config.isCoordinatorWriteOmissionOnReadOnlyEnabled();
    crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            config.isIncludeMetadataEnabled(),
            config.isIndexEventuallyConsistentReadEnabled(),
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

    ConsensusCommitUtils.warnIfBeforeIndexesAreMissing(admin, config);

    backupStalenessBoundMillis = config.getBackupStalenessBoundMillis();
    transactionTimeoutMillis = config.getTransactionTimeoutMillis();
    backupModeDaemon = new BackupModeDaemon(coordinator, config.getBackupCheckIntervalMillis());
    backupModeDaemon.start();
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
    this.coordinatorWriteOmissionOnReadOnlyEnabled =
        config.isCoordinatorWriteOmissionOnReadOnlyEnabled();
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
    this.backupStalenessBoundMillis = config.getBackupStalenessBoundMillis();
    this.transactionTimeoutMillis = config.getTransactionTimeoutMillis();
    // The test-only constructor does not run the backup-mode daemon; begin() then captures no
    // label.
    this.backupModeDaemon = null;
  }

  // `groupCommitter` must be set before calling this method.
  private CommitHandler createCommitHandler(
      ConsensusCommitConfig config, StorageInfoProvider storageInfoProvider) {
    MutationsGrouper mutationsGrouper = new MutationsGrouper(storageInfoProvider);
    CommitHandler handler;
    if (isGroupCommitEnabled()) {
      handler =
          new CommitHandlerWithGroupCommit(
              storage,
              coordinator,
              tableMetadataManager,
              parallelExecutor,
              mutationsGrouper,
              config.isCoordinatorWriteOmissionOnReadOnlyEnabled(),
              config.isOnePhaseCommitEnabled(),
              groupCommitter);
    } else {
      handler =
          new CommitHandler(
              storage,
              coordinator,
              tableMetadataManager,
              parallelExecutor,
              mutationsGrouper,
              config.isCoordinatorWriteOmissionOnReadOnlyEnabled(),
              config.isOnePhaseCommitEnabled());
    }
    return handler;
  }

  @Override
  public DistributedTransaction begin(String txId, Map<String, String> attributes) {
    return begin(
        txId,
        ConsensusCommitOperationAttributes.getTransactionIsolation(attributes).orElse(isolation),
        false,
        false);
  }

  @Override
  public DistributedTransaction beginReadOnly(String txId, Map<String, String> attributes) {
    return begin(
        txId,
        ConsensusCommitOperationAttributes.getTransactionIsolation(attributes).orElse(isolation),
        true,
        false);
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
    // Reserve a group commit slot for every non-read-only transaction, and for a read-only
    // transaction only when coordinator write omission is disabled. Such a read-only transaction
    // writes a coordinator state row, so without a slot it would reach the group commit path with a
    // bare (unreserved) transaction ID and fail. A non-read-only transaction that turns out to be
    // write-less under write omission still reserves a slot here but cancels it later on the commit
    // path.
    boolean groupCommitSlotReserved = false;
    if (isGroupCommitEnabled() && (!readOnly || !coordinatorWriteOmissionOnReadOnlyEnabled)) {
      assert groupCommitter != null;
      txId = groupCommitter.reserve(txId);
      groupCommitSlotReserved = true;
    }
    // Capture the active backup label from the daemon cache once, at begin. A stale cache is
    // refreshed synchronously first so a process resuming from a stall cannot begin under a stale
    // flag.
    String activeBackupLabel =
        backupModeDaemon == null
            ? null
            : backupModeDaemon.activeBackupLabel(backupStalenessBoundMillis);
    Snapshot snapshot = new Snapshot(txId, tableMetadataManager, parallelExecutor);
    TransactionContext context =
        new TransactionContext(
            txId,
            snapshot,
            isolation,
            readOnly,
            oneOperation,
            groupCommitSlotReserved,
            activeBackupLabel,
            System.nanoTime(),
            transactionTimeoutMillis);
    DistributedTransaction transaction =
        new ConsensusCommit(context, crud, commit, operationChecker, groupCommitter);
    if (readOnly) {
      transaction = new ReadOnlyDistributedTransaction(transaction);
    }
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return transaction;
  }

  private DistributedTransaction beginOneOperation(
      boolean readOnly, Map<String, String> attributes) {
    String txId = UUID.randomUUID().toString();
    return begin(
        txId,
        ConsensusCommitOperationAttributes.getTransactionIsolation(attributes).orElse(isolation),
        readOnly,
        true);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)), true, get.getAttributes());
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(
        t -> t.scan(copyAndSetTargetToIfNot(scan)), true, scan.getAttributes());
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    DistributedTransaction transaction = beginOneOperation(true, scan.getAttributes());

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

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(put));
          return null;
        },
        false,
        put.getAttributes());
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(puts));
          return null;
        },
        false,
        puts.isEmpty() ? Collections.emptyMap() : puts.get(0).getAttributes());
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.insert(copyAndSetTargetToIfNot(insert));
          return null;
        },
        false,
        insert.getAttributes());
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.upsert(copyAndSetTargetToIfNot(upsert));
          return null;
        },
        false,
        upsert.getAttributes());
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.update(copyAndSetTargetToIfNot(update));
          return null;
        },
        false,
        update.getAttributes());
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(delete));
          return null;
        },
        false,
        delete.getAttributes());
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(deletes));
          return null;
        },
        false,
        deletes.isEmpty() ? Collections.emptyMap() : deletes.get(0).getAttributes());
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.mutate(copyAndSetTargetToIfNot(mutations));
          return null;
        },
        false,
        mutations.isEmpty() ? Collections.emptyMap() : mutations.get(0).getAttributes());
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations)
      throws CrudException, UnknownTransactionStatusException {
    boolean readOnly = operations.stream().allMatch(o -> o instanceof Selection);
    return executeTransaction(
        t -> t.batch(operations),
        readOnly,
        operations.isEmpty() ? Collections.emptyMap() : operations.get(0).getAttributes());
  }

  private <R> R executeTransaction(
      ThrowableFunction<DistributedTransaction, R, TransactionException> throwableFunction,
      boolean readOnly,
      Map<String, String> attributes)
      throws CrudException, UnknownTransactionStatusException {
    DistributedTransaction transaction = beginOneOperation(readOnly, attributes);

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
      return commit.abortStateForRollback(txId);
    } catch (UnknownTransactionStatusException ignored) {
      return TransactionState.UNKNOWN;
    }
  }

  @Override
  public boolean finishTransaction(String txId) throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));

    // Read the Coordinator state row for this transaction.
    Optional<State> stateOpt;
    try {
      stateOpt = coordinator.getState(txId);
    } catch (CoordinatorException e) {
      throw new TransactionException(
          CoreError.CONSENSUS_COMMIT_FINISHING_TRANSACTION_FAILED.buildMessage(
              txId, e.getMessage()),
          e,
          txId);
    }

    // The state row is absent — the transaction is already finished, was never started, or was
    // cleaned up by a concurrent caller. finishTransaction is idempotent on that boundary, so
    // report success.
    if (!stateOpt.isPresent()) {
      return true;
    }

    State state = stateOpt.get();

    // The transaction did not commit through DistributedTransaction#commit() (e.g., it was
    // terminated via DistributedTransactionManager#rollback()/abort(), aborted by lazy recovery, or
    // originated from a pre-feature binary) and so carries no write set. Note that a commit() that
    // failed during preparation is ABORTED but still carries a write set, so it is not in this
    // category. This is not an error: the transaction is simply not applicable to finishTransaction
    // (retrying with the same txId would never succeed), so report it via the return value and
    // leave the state row for lazy recovery.
    if (!state.getWriteSet().isPresent()) {
      return false;
    }

    // Walk every EntryGroup and run per-record recovery. The decoder rebuilds a Get for each entry;
    // the recovery executor decides rollforward vs. rollback based on the parent state. Records
    // that are already terminal or belong to a different transaction are filtered out up front. The
    // composer-level NoMutationException absorption still acts as the safety net for races against
    // concurrent recovery.
    WriteSet writeSet = state.getWriteSet().get();
    for (EntryGroup entryGroup : writeSet.getEntryGroupsList()) {
      String expectedTxId =
          entryGroup.hasChildId()
              ? KEY_MANIPULATOR.fullKey(state.getId(), entryGroup.getChildId())
              : state.getId();
      for (Entry entry : entryGroup.getEntriesList()) {
        Get get = WriteSetDecoder.toGet(entry);
        Optional<Result> resultOpt;
        try {
          resultOpt = storage.get(get);
        } catch (ExecutionException e) {
          throw new TransactionException(
              CoreError.CONSENSUS_COMMIT_FINISHING_TRANSACTION_FAILED.buildMessage(
                  txId, e.getMessage()),
              e,
              txId);
        }
        if (!resultOpt.isPresent()) {
          // Record is gone (concurrently deleted, or it was a Delete entry whose target had not yet
          // been written). Nothing to recover.
          continue;
        }
        TransactionResult txResult = new TransactionResult(resultOpt.get());
        if (!shouldRecover(txResult, expectedTxId)) {
          // Already finalized, or overwritten by an unrelated transaction. Recovery would no-op.
          continue;
        }
        try {
          recoveryExecutor.executeSynchronously(get, txResult, state);
        } catch (ExecutionException e) {
          throw new TransactionException(
              CoreError.CONSENSUS_COMMIT_FINISHING_TRANSACTION_FAILED.buildMessage(
                  txId, e.getMessage()),
              e,
              txId);
        }
      }
    }

    // Perform the Coordinator state cleanup (delete the state row). state.getId() is the parent ID
    // for group-commit rows because Coordinator#getState(fullChildId) routes through
    // getStateForGroupCommit() and returns the parent row, so the delete targets the right row in
    // both single and group cases.
    // Coordinator#deleteState is unconditional and benign on a concurrent delete.
    try {
      coordinator.deleteState(state.getId());
    } catch (CoordinatorException e) {
      throw new TransactionException(
          CoreError.CONSENSUS_COMMIT_FINISHING_TRANSACTION_FAILED.buildMessage(
              txId, e.getMessage()),
          e,
          txId);
    }

    return true;
  }

  /**
   * Returns {@code true} when the record needs recovery as part of finishing the given transaction:
   * it must still be in a non-terminal state ({@code PREPARED} or {@code DELETED}) <em>and</em>
   * carry the transaction id we are finishing. If either condition fails, the record has either
   * been finalized already or has been overwritten by an unrelated transaction, and the recovery
   * mutation would no-op via {@code NoMutationException} anyway.
   *
   * @param result the storage-layer result for the record
   * @param expectedTxId the transaction id whose write set is being finished. For non-group-commit
   *     this is the transaction id itself; for group-commit it is the full id formed from the
   *     parent id and the entry group's child id
   * @return {@code true} if {@code recoveryExecutor.executeSynchronously} should be invoked for
   *     this record
   */
  private static boolean shouldRecover(TransactionResult result, String expectedTxId) {
    TransactionState recordState = result.getState();
    if (recordState != TransactionState.PREPARED && recordState != TransactionState.DELETED) {
      return false;
    }
    return expectedTxId.equals(result.getId());
  }

  @Override
  public boolean recoverRecord(
      String namespace, String table, Key partitionKey, @Nullable Key clusteringKey)
      throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(namespace));
    checkArgument(!Strings.isNullOrEmpty(table));
    checkNotNull(partitionKey);

    // Read the current physical state of the record.
    Get get = buildRecordGet(namespace, table, partitionKey, clusteringKey);
    Optional<Result> resultOpt;
    try {
      resultOpt = storage.get(get);
    } catch (ExecutionException e) {
      throw new TransactionException(
          CoreError.CONSENSUS_COMMIT_RECOVERING_RECORD_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table),
              partitionKey,
              clusteringKey,
              e.getMessage()),
          e,
          null);
    }

    // Nothing to recover if the record does not exist or is already committed. isCommitted() also
    // covers a record with no transaction metadata (tx_state absent), which is deemed as committed.
    // The record is already resolved in both cases, so report it as recovered.
    if (!resultOpt.isPresent()) {
      return true;
    }
    TransactionResult txResult = new TransactionResult(resultOpt.get());
    if (txResult.isCommitted()) {
      return true;
    }

    // The record is uncommitted (PREPARED or DELETED). A record written by ScalarDB always carries
    // both tx_id and tx_state together, so tx_id is non-null here; the isCommitted() check above
    // already handled the no-metadata case where tx_state — and thus tx_id — is absent.
    String txId = txResult.getId();
    assert txId != null;

    Optional<State> stateOpt;
    try {
      stateOpt = coordinator.getState(txId);
    } catch (CoordinatorException e) {
      throw new TransactionException(
          CoreError.CONSENSUS_COMMIT_RECOVERING_RECORD_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table),
              partitionKey,
              clusteringKey,
              e.getMessage()),
          e,
          txId);
    }

    // Recover the single record and report whether it was resolved. The Coordinator state cleanup
    // is deliberately not performed: the writer may span other records that still need the state
    // row, and the transaction-wide ABORTED row written for the no-state case keeps those records
    // consistent via lazy recovery.
    try {
      return recoveryExecutor.executeSynchronously(get, txResult, stateOpt);
    } catch (ExecutionException | CoordinatorException e) {
      throw new TransactionException(
          CoreError.CONSENSUS_COMMIT_RECOVERING_RECORD_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table),
              partitionKey,
              clusteringKey,
              e.getMessage()),
          e,
          txId);
    }
  }

  private static Get buildRecordGet(
      String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    // Read all columns (no projections) with linearizable consistency so the before-image and the
    // transaction metadata needed for recovery are available.
    GetBuilder.BuildableGetWithPartitionKey builder =
        Get.newBuilder()
            .namespace(namespace)
            .table(table)
            .partitionKey(partitionKey)
            .consistency(Consistency.LINEARIZABLE);
    if (clusteringKey != null) {
      builder.clusteringKey(clusteringKey);
    }
    return builder.build();
  }

  @VisibleForTesting
  boolean isGroupCommitEnabled() {
    return groupCommitter != null;
  }

  @Override
  public void enableRedoLogging(String backupLabel) {
    checkArgument(
        backupLabel != null && !backupLabel.isEmpty(), "backupLabel must not be null or empty");
    try {
      coordinator.enterBackupMode(backupLabel, backupUpdatedBy);
    } catch (CoordinatorConflictException e) {
      // A row for this label already exists. Treat an already-open window as success (idempotent);
      // reject a label whose window has already been closed or otherwise transitioned.
      if (!isBackingUp(backupLabel)) {
        throw new IllegalStateException(
            "A backup window for label '" + backupLabel + "' already exists and is not BACKING_UP",
            e);
      }
    } catch (CoordinatorException e) {
      throw new IllegalStateException(
          "Failed to open the backup window for label '" + backupLabel + "'", e);
    }
    openBackupLabel = backupLabel;
    refreshBackupCache();
  }

  @Override
  public void disableRedoLogging() {
    String label = openBackupLabel;
    if (label != null) {
      try {
        coordinator.exitBackupMode(label, backupUpdatedBy);
      } catch (CoordinatorConflictException e) {
        // The window is not BACKING_UP (already closed or never opened here). Nothing to close.
      } catch (CoordinatorException e) {
        throw new IllegalStateException(
            "Failed to close the backup window for label '" + label + "'", e);
      }
      openBackupLabel = null;
    }
    refreshBackupCache();
  }

  // Whether the backup table currently holds a BACKING_UP row for the given label.
  private boolean isBackingUp(String backupLabel) {
    try {
      return coordinator.scanBackingUpLabels().contains(backupLabel);
    } catch (CoordinatorException e) {
      return false;
    }
  }

  // Refreshes the local daemon cache so this process's own begin() sees the transition immediately,
  // without waiting for the next periodic scan.
  private void refreshBackupCache() {
    if (backupModeDaemon != null) {
      backupModeDaemon.refreshNow();
    }
  }

  @Override
  public void close() {
    storage.close();
    admin.close();
    parallelExecutor.close();
    recoveryExecutor.close();
    if (backupModeDaemon != null) {
      backupModeDaemon.close();
    }
    if (isGroupCommitEnabled()) {
      assert groupCommitter != null;
      groupCommitter.close();
    }
  }
}
