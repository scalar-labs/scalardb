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
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.ThrowableFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TwoPhaseConsensusCommitManager
    extends ActiveTransactionManagedTwoPhaseCommitTransactionManager {

  private static final Logger logger =
      LoggerFactory.getLogger(TwoPhaseConsensusCommitManager.class);

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
    commit = new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
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
    commit = new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
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
    this.recovery = recovery;
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
    throwIfGroupCommitIsEnabled();
    return createNewTransaction(txId, isolation, strategy, true);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    checkArgument(!Strings.isNullOrEmpty(txId));
    return join(txId, config.getIsolation(), config.getSerializableStrategy());
  }

  @VisibleForTesting
  TwoPhaseCommitTransaction join(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    throwIfGroupCommitIsEnabled();
    // If the transaction associated with the specified transaction ID is active, resume it
    if (isTransactionActive(txId)) {
      return resume(txId);
    }

    return createNewTransaction(txId, isolation, strategy, true);
  }

  private TwoPhaseCommitTransaction createNewTransaction(
      String txId, Isolation isolation, SerializableStrategy strategy, boolean decorate)
      throws TransactionException {
    Snapshot snapshot =
        new Snapshot(txId, isolation, strategy, tableMetadataManager, parallelExecutor);
    CrudHandler crud =
        new CrudHandler(
            storage, snapshot, tableMetadataManager, isIncludeMetadataEnabled, parallelExecutor);

    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, mutationOperationChecker);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);
    return decorate ? decorate(transaction) : transaction;
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
      ThrowableFunction<TwoPhaseCommitTransaction, R, TransactionException> throwableFunction)
      throws CrudException, UnknownTransactionStatusException {
    TwoPhaseCommitTransaction transaction;
    try {
      transaction = beginInternal();
    } catch (TransactionNotFoundException e) {
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (TransactionException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

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

  @VisibleForTesting
  TwoPhaseCommitTransaction beginInternal() throws TransactionException {
    String txId = UUID.randomUUID().toString();
    return createNewTransaction(
        txId, config.getIsolation(), config.getSerializableStrategy(), false);
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
  }
}
