package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractTransactionManagerCrudOperableScanner;
import com.scalar.db.common.AbstractTwoPhaseCommitTransactionManager;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TwoPhaseConsensusCommitManager extends AbstractTwoPhaseCommitTransactionManager {

  private static final Logger logger =
      LoggerFactory.getLogger(TwoPhaseConsensusCommitManager.class);

  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final ConsensusCommitConfig config;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final Coordinator coordinator;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryExecutor recoveryExecutor;
  private final CommitHandler commit;
  private final boolean isIncludeMetadataEnabled;
  private final ConsensusCommitMutationOperationChecker mutationOperationChecker;

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
    RecoveryHandler recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    recoveryExecutor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);
    commit =
        new CommitHandler(
            storage,
            coordinator,
            tableMetadataManager,
            parallelExecutor,
            new MutationsGrouper(new StorageInfoProvider(admin)),
            config.isCoordinatorWriteOmissionOnReadOnlyEnabled());
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
    RecoveryHandler recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    recoveryExecutor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);
    commit =
        new CommitHandler(
            storage,
            coordinator,
            tableMetadataManager,
            parallelExecutor,
            new MutationsGrouper(new StorageInfoProvider(admin)),
            config.isCoordinatorWriteOmissionOnReadOnlyEnabled());
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
      RecoveryExecutor recoveryExecutor,
      CommitHandler commit) {
    super(databaseConfig);
    this.storage = storage;
    this.admin = admin;
    this.config = config;
    tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    this.coordinator = coordinator;
    this.parallelExecutor = parallelExecutor;
    this.recoveryExecutor = recoveryExecutor;
    this.commit = commit;
    isIncludeMetadataEnabled = config.isIncludeMetadataEnabled();
    mutationOperationChecker = new ConsensusCommitMutationOperationChecker(tableMetadataManager);
  }

  private void throwIfGroupCommitIsEnabled() {
    if (CoordinatorGroupCommitter.isEnabled(config)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_GROUP_COMMIT_WITH_TWO_PHASE_COMMIT_INTERFACE_NOT_ALLOWED
              .buildMessage());
    }
  }

  @Override
  public TwoPhaseCommitTransaction begin() {
    String txId = UUID.randomUUID().toString();
    return begin(txId, config.getIsolation());
  }

  @Override
  public TwoPhaseCommitTransaction begin(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return begin(txId, config.getIsolation());
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction begin(Isolation isolation) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation);
  }

  private TwoPhaseCommitTransaction begin(String txId, Isolation isolation) {
    throwIfGroupCommitIsEnabled();
    return begin(txId, isolation, false, false);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return join(txId, config.getIsolation());
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction join(String txId, Isolation isolation) {
    throwIfGroupCommitIsEnabled();
    return begin(txId, isolation, false, false);
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction begin(
      String txId, Isolation isolation, boolean readOnly, boolean oneOperation) {
    Snapshot snapshot = new Snapshot(txId, isolation, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(
            storage,
            snapshot,
            recoveryExecutor,
            tableMetadataManager,
            isIncludeMetadataEnabled,
            parallelExecutor,
            readOnly,
            oneOperation);
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, mutationOperationChecker);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return transaction;
  }

  private TwoPhaseCommitTransaction beginOneOperation(boolean readOnly) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, config.getIsolation(), readOnly, true);
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
    TwoPhaseCommitTransaction transaction = beginOneOperation(true);

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
          transaction.prepare();
          transaction.validate();
          transaction.commit();
        } catch (PreparationConflictException
            | ValidationConflictException
            | CommitConflictException e) {
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

  private <R> R executeTransaction(
      ThrowableFunction<TwoPhaseCommitTransaction, R, TransactionException> throwableFunction,
      boolean readOnly)
      throws CrudException, UnknownTransactionStatusException {
    TwoPhaseCommitTransaction transaction = beginOneOperation(readOnly);
    try {
      R result = throwableFunction.apply(transaction);
      transaction.prepare();
      transaction.validate();
      transaction.commit();
      return result;
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    } catch (PreparationConflictException
        | ValidationConflictException
        | CommitConflictException e) {
      rollbackTransaction(transaction);
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (UnknownTransactionStatusException e) {
      throw e;
    } catch (TransactionException e) {
      rollbackTransaction(transaction);
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  private void rollbackTransaction(TwoPhaseCommitTransaction transaction) {
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

  @Override
  public void close() {
    storage.close();
    admin.close();
    parallelExecutor.close();
    recoveryExecutor.close();
  }
}
