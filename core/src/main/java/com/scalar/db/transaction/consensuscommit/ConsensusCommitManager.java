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
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractDistributedTransactionManager;
import com.scalar.db.common.AbstractTransactionManagerCrudOperableScanner;
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

  protected ConsensusCommitManager(DatabaseConfig databaseConfig) {
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
      @Nullable CoordinatorGroupCommitter groupCommitter) {
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
  public DistributedTransaction begin() {
    return begin(config.getIsolation());
  }

  @Override
  public DistributedTransaction begin(String txId) {
    return begin(txId, config.getIsolation());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(com.scalar.db.api.Isolation isolation) {
    return begin(Isolation.valueOf(isolation.name()));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, com.scalar.db.api.Isolation isolation) {
    return begin(txId, Isolation.valueOf(isolation.name()));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      com.scalar.db.api.Isolation isolation, com.scalar.db.api.SerializableStrategy strategy) {
    return begin(Isolation.valueOf(isolation.name()));
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(com.scalar.db.api.SerializableStrategy strategy) {
    return begin(Isolation.SERIALIZABLE);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, com.scalar.db.api.SerializableStrategy strategy) {
    return begin(txId, Isolation.SERIALIZABLE);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId,
      com.scalar.db.api.Isolation isolation,
      com.scalar.db.api.SerializableStrategy strategy) {
    return begin(txId, Isolation.valueOf(isolation.name()));
  }

  @VisibleForTesting
  DistributedTransaction begin(Isolation isolation) {
    String txId = UUID.randomUUID().toString();
    return begin(txId, isolation);
  }

  @VisibleForTesting
  DistributedTransaction begin(String txId, Isolation isolation) {
    checkArgument(!Strings.isNullOrEmpty(txId));
    checkNotNull(isolation);
    if (isGroupCommitEnabled()) {
      assert groupCommitter != null;
      txId = groupCommitter.reserve(txId);
    }
    if (!config.getIsolation().equals(isolation)) {
      logger.warn(
          "Setting different isolation level from the one in DatabaseConfig might cause unexpected "
              + "anomalies");
    }
    Snapshot snapshot = new Snapshot(txId, isolation, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(
            storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled, parallelExecutor);
    ConsensusCommit consensus =
        new ConsensusCommit(crud, commit, recovery, mutationOperationChecker, groupCommitter);
    getNamespace().ifPresent(consensus::withNamespace);
    getTable().ifPresent(consensus::withTable);
    return consensus;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.scan(copyAndSetTargetToIfNot(scan)));
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    DistributedTransaction transaction = begin();

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
        });
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
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

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
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
    DistributedTransaction transaction = begin();
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
      assert groupCommitter != null;
      groupCommitter.close();
    }
  }
}
