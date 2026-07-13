package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.BatchResultImpl;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.ScannerIterator;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.VirtualTableInfoManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consensus-commit-backed {@link TwoPhaseCommit.Participant} implementation.
 *
 * <p>Maintains a per-transaction {@link TransactionContext} keyed by the canonical transaction ID
 * supplied by the Coordinator. The context is created by {@link #join} and removed once the
 * participant has run the last step the Coordinator drives for it: {@link #commitRecords} or {@link
 * #rollbackRecords} for a participant with writes, or an earlier step for a write-less participant
 * whose later steps the Coordinator skips — {@link #validateRecords}, or {@link #prepareRecords}
 * when validation is not required either.
 *
 * <p>This implementation requires a stable {@code participantId} (see {@link
 * ConsensusCommitConfig#PARTICIPANT_ID}) so that {@link TwoPhaseCommit.WriteSetEntry} instances
 * carry the participant attribution needed by active recovery. The constructor throws if the
 * property is unset.
 *
 * <p>State writes on the Coordinator table are <strong>not</strong> performed by this class — those
 * are the {@link TwoPhaseCommit.Coordinator}'s responsibility under the new model. This class only
 * touches participant-owned records via {@link CrudHandler} and {@link ParticipantCommitHandler}'s
 * record-level primitives ({@code prepareRecords}, {@code validateRecords}, {@code commitRecords},
 * {@code rollbackRecords}).
 */
@ThreadSafe
public class ConsensusCommitParticipant implements TwoPhaseCommit.Participant {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusCommitParticipant.class);

  private final ConsensusCommitConfig config;
  private final String participantId;
  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final RecoveryExecutor recoveryExecutor;
  private final CrudHandler crud;
  private final ParticipantCommitHandler commit;
  private final ConsensusCommitOperationChecker operationChecker;

  private final ConcurrentMap<String, ParticipantContext> contexts = new ConcurrentHashMap<>();

  public ConsensusCommitParticipant(DatabaseConfig databaseConfig) {
    this.config = new ConsensusCommitConfig(databaseConfig);
    this.participantId = resolveParticipantId(config);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    this.storage = storageFactory.getStorage();
    this.admin = storageFactory.getStorageAdmin();
    this.tableMetadataManager =
        new TransactionTableMetadataManager(
            admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    CoordinatorStateAccessor coordinator = new CoordinatorStateAccessor(storage, config);
    this.parallelExecutor = new ParallelExecutor(config);
    RecoveryHandler recovery = new RecoveryHandler(storage, coordinator, tableMetadataManager);
    this.recoveryExecutor =
        new RecoveryExecutor(storage, coordinator, recovery, tableMetadataManager);
    this.crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            config.isIncludeMetadataEnabled(),
            config.isIndexEventuallyConsistentReadEnabled(),
            parallelExecutor);
    StorageInfoProvider storageInfoProvider = new StorageInfoProvider(admin);
    this.commit =
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            parallelExecutor,
            new MutationsGrouper(storageInfoProvider),
            config.isOnePhaseCommitEnabled());
    VirtualTableInfoManager virtualTableInfoManager =
        new VirtualTableInfoManager(admin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    this.operationChecker =
        new ConsensusCommitOperationChecker(
            tableMetadataManager,
            virtualTableInfoManager,
            storageInfoProvider,
            config.isIncludeMetadataEnabled());
  }

  @VisibleForTesting
  ConsensusCommitParticipant(
      ConsensusCommitConfig config,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      RecoveryExecutor recoveryExecutor,
      CrudHandler crud,
      ParticipantCommitHandler commit,
      ConsensusCommitOperationChecker operationChecker) {
    this.config = checkNotNull(config);
    this.participantId = resolveParticipantId(config);
    this.storage = null;
    this.admin = null;
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.crud = checkNotNull(crud);
    this.commit = checkNotNull(commit);
    this.operationChecker = checkNotNull(operationChecker);
  }

  private static String resolveParticipantId(ConsensusCommitConfig config) {
    return config
        .getParticipantId()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    CoreError.CONSENSUS_COMMIT_PARTICIPANT_ID_IS_REQUIRED.buildMessage()));
  }

  @Override
  public String getId() {
    return participantId;
  }

  @Override
  public void join(String transactionId, boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    Snapshot snapshot = new Snapshot(transactionId, tableMetadataManager, parallelExecutor);
    // The isolation level can be overridden per transaction via the transaction-isolation
    // attribute; otherwise it falls back to the participant's configured default.
    Isolation isolation =
        ConsensusCommitOperationAttributes.getTransactionIsolation(attributes)
            .orElse(config.getIsolation());
    TransactionContext context =
        new TransactionContext(transactionId, snapshot, isolation, readOnly, false);
    ParticipantContext existing =
        contexts.putIfAbsent(transactionId, new ParticipantContext(context));
    if (existing != null) {
      throw new TransactionException(
          CoreError.TRANSACTION_ALREADY_EXISTS.buildMessage(), transactionId);
    }
  }

  @Override
  public Optional<Result> get(String transactionId, Get get)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      try {
        operationChecker.check(get, context);
      } catch (ExecutionException e) {
        throw new CrudException(e.getMessage(), e, transactionId);
      }
      return crud.get(get, context);
    }
  }

  @Override
  public List<Result> scan(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      try {
        operationChecker.check(scan, context);
      } catch (ExecutionException e) {
        throw new CrudException(e.getMessage(), e, transactionId);
      }
      return crud.scan(scan, context);
    }
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      try {
        operationChecker.check(scan, context);
      } catch (ExecutionException e) {
        throw new CrudException(e.getMessage(), e, transactionId);
      }
      return new SynchronizedParticipantScanner(pc, crud.getScanner(scan, context));
    }
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(String transactionId, Put put)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      checkWritable(transactionId, context);
      checkMutation(transactionId, put);
      crud.put(put, context);
    }
  }

  @Override
  public void insert(String transactionId, Insert insert)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      checkWritable(transactionId, context);
      Put put = ConsensusCommitUtils.createPutForInsert(insert);
      checkMutation(transactionId, put);
      crud.put(put, context);
    }
  }

  @Override
  public void upsert(String transactionId, Upsert upsert)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      checkWritable(transactionId, context);
      Put put = ConsensusCommitUtils.createPutForUpsert(upsert);
      checkMutation(transactionId, put);
      crud.put(put, context);
    }
  }

  @Override
  public void update(String transactionId, Update update)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      checkWritable(transactionId, context);
      ScalarDbUtils.checkUpdate(update);
      Put put = ConsensusCommitUtils.createPutForUpdate(update);
      checkMutation(transactionId, put);
      try {
        crud.put(put, context);
      } catch (UnsatisfiedConditionException e) {
        if (update.getCondition().isPresent()) {
          throw new UnsatisfiedConditionException(
              ConsensusCommitUtils.convertUnsatisfiedConditionExceptionMessageForUpdate(
                  e, update.getCondition().get()),
              transactionId);
        }
        // If the condition is not specified, the record does not exist; do nothing.
      }
    }
  }

  @Override
  public void delete(String transactionId, Delete delete)
      throws CrudException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      checkWritable(transactionId, context);
      checkMutation(transactionId, delete);
      crud.delete(delete, context);
    }
  }

  @Override
  public void mutate(String transactionId, List<? extends Mutation> mutations)
      throws CrudException, TransactionNotFoundException {
    if (mutations.isEmpty()) {
      throw new IllegalArgumentException(CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    }
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put(transactionId, (Put) mutation);
      } else if (mutation instanceof Insert) {
        insert(transactionId, (Insert) mutation);
      } else if (mutation instanceof Upsert) {
        upsert(transactionId, (Upsert) mutation);
      } else if (mutation instanceof Update) {
        update(transactionId, (Update) mutation);
      } else if (mutation instanceof Delete) {
        delete(transactionId, (Delete) mutation);
      } else {
        throw new AssertionError("Unknown mutation: " + mutation);
      }
    }
  }

  @Override
  public List<CrudOperable.BatchResult> batch(
      String transactionId, List<? extends Operation> operations)
      throws CrudException, TransactionNotFoundException {
    if (operations.isEmpty()) {
      throw new IllegalArgumentException(CoreError.EMPTY_OPERATIONS_SPECIFIED.buildMessage());
    }
    List<CrudOperable.BatchResult> results = new ArrayList<>(operations.size());
    for (Operation operation : operations) {
      if (operation instanceof Get) {
        Optional<Result> result = get(transactionId, (Get) operation);
        results.add(new BatchResultImpl(result));
      } else if (operation instanceof Scan) {
        List<Result> scanned = scan(transactionId, (Scan) operation);
        results.add(new BatchResultImpl(scanned));
      } else if (operation instanceof Insert) {
        insert(transactionId, (Insert) operation);
        results.add(BatchResultImpl.INSERT_BATCH_RESULT);
      } else if (operation instanceof Upsert) {
        upsert(transactionId, (Upsert) operation);
        results.add(BatchResultImpl.UPSERT_BATCH_RESULT);
      } else if (operation instanceof Update) {
        update(transactionId, (Update) operation);
        results.add(BatchResultImpl.UPDATE_BATCH_RESULT);
      } else if (operation instanceof Delete) {
        delete(transactionId, (Delete) operation);
        results.add(BatchResultImpl.DELETE_BATCH_RESULT);
      } else if (operation instanceof Put) {
        put(transactionId, (Put) operation);
        results.add(BatchResultImpl.PUT_BATCH_RESULT);
      } else {
        throw new AssertionError("Unknown operation: " + operation);
      }
    }
    return results;
  }

  @Override
  public TwoPhaseCommit.PreparationResult prepareRecords(
      String transactionId, long preparedAt, TwoPhaseCommit.WriteSetDetailLevel detailLevel)
      throws PreparationException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkActive();
      TransactionContext context = pc.context();
      if (!context.areAllScannersClosed()) {
        throw new IllegalStateException(
            CoreError.CONSENSUS_COMMIT_SCANNER_NOT_CLOSED.buildMessage());
      }
      try {
        crud.readIfImplicitPreReadEnabled(context);
      } catch (CrudConflictException e) {
        throw new PreparationConflictException(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHILE_IMPLICIT_PRE_READ.buildMessage(
                e.getMessage()),
            e,
            transactionId);
      } catch (CrudException e) {
        throw new PreparationException(
            CoreError.CONSENSUS_COMMIT_EXECUTING_IMPLICIT_PRE_READ_FAILED.buildMessage(
                e.getMessage()),
            e,
            transactionId);
      }
      try {
        crud.waitForRecoveryCompletionIfNecessary(context);
      } catch (CrudConflictException e) {
        throw new PreparationConflictException(e.getMessage(), e, transactionId);
      } catch (CrudException e) {
        throw new PreparationException(e.getMessage(), e, transactionId);
      }
      try {
        commit.prepareRecords(context, preparedAt);
      } finally {
        // Mark PREPARED even if prepareRecords throws partway: a partial prepare leaves PREPARED
        // records in storage that rollbackRecords must undo (it gates the storage rollback on this
        // status). Mirrors TwoPhaseConsensusCommit.prepare, which sets needRollback in a finally.
        pc.toPrepared();
      }
      List<TwoPhaseCommit.WriteSetEntry> writeSet = buildWriteSetEntries(context, detailLevel);
      boolean validationRequired = context.isValidationRequired();
      boolean commitRequired = context.isCommitRequired();
      // The Coordinator skips validateRecords when validation is not required and commitRecords
      // when commit is not required. When both are skipped, prepareRecords is this participant's
      // last step, so release the context now (a write-less participant has written no PREPARED
      // record). The status thus goes ACTIVE -> PREPARED -> RELEASED within this synchronized
      // block; the transient PREPARED is harmless because a concurrent rollbackRecords sees
      // RELEASED and no-ops.
      if (!validationRequired && !commitRequired) {
        release(transactionId, pc);
      }
      return new PreparationResultImpl(writeSet, validationRequired, commitRequired);
    }
  }

  @Override
  public void validateRecords(String transactionId)
      throws ValidationException, TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkPrepared();
      TransactionContext context = pc.context();
      commit.validateRecords(context);
      if (context.isCommitRequired()) {
        pc.toValidated();
      } else {
        // The Coordinator skips commitRecords for a write-less participant, so validateRecords is
        // this participant's last step. Release the context now (no PREPARED records to commit).
        // This stays in agreement with the Coordinator's commit-step skip by construction: both
        // this self-release and PreparationResult#isCommitRequired derive from
        // context.isCommitRequired(), so the Coordinator skips commitRecords exactly when this
        // branch has already released the context (otherwise it could drive commitRecords on a
        // released context -> TNF).
        release(transactionId, pc);
      }
    }
  }

  @Override
  public void commitRecords(String transactionId, long committedAt)
      throws TransactionNotFoundException {
    ParticipantContext pc = getParticipantContext(transactionId);
    synchronized (pc) {
      pc.checkPreparedOrValidated();
      try {
        commit.commitRecords(pc.context(), committedAt);
      } finally {
        // The Coordinator has durably decided COMMITTED; release the context even on failure (lazy
        // recovery commits the records via the COMMITTED Coordinator state).
        release(transactionId, pc);
      }
    }
  }

  @Override
  public void rollbackRecords(String transactionId) {
    ParticipantContext pc = contexts.get(transactionId);
    if (pc == null) {
      // Unknown or already-released transaction (e.g., a write-less participant that released early
      // when its later steps were skipped, or a prior rollback): nothing to undo. Lenient no-op,
      // mirroring Coordinator#rollback.
      return;
    }
    synchronized (pc) {
      if (pc.isReleased()) {
        // Released by a concurrent terminal step after we fetched the reference; nothing to undo.
        return;
      }
      TransactionContext context = pc.context();
      try {
        try {
          context.closeScanners();
        } catch (CrudException e) {
          logger.warn("Failed to close the scanner. Transaction ID: {}", transactionId, e);
        }
        if (pc.isPrepared()) {
          // Only a prepared transaction has PREPARED records in storage to roll back. An unprepared
          // transaction has only an in-memory snapshot, discarded by removing the context below.
          commit.rollbackRecords(context);
        }
      } finally {
        release(transactionId, pc);
      }
    }
  }

  @Override
  public boolean hasTransactionContext(String transactionId) {
    return contexts.containsKey(transactionId);
  }

  @Override
  public void releaseTransactionContext(String transactionId) {
    ParticipantContext pc = contexts.get(transactionId);
    if (pc == null) {
      // Unknown or already-released transaction: nothing to release.
      return;
    }
    synchronized (pc) {
      if (pc.isReleased()) {
        // Released by a concurrent terminal step after we fetched the reference; nothing to do.
        return;
      }
      TransactionContext context = pc.context();
      try {
        // Reap-only terminal: close scanners and discard the in-memory snapshot WITHOUT touching
        // storage. Even if the transaction is PREPARED, we must not roll its records back here (the
        // outcome may be undetermined or COMMITTED); lazy recovery reconciles them on a later read.
        context.closeScanners();
      } catch (CrudException e) {
        logger.warn("Failed to close the scanner. Transaction ID: {}", transactionId, e);
      } finally {
        release(transactionId, pc);
      }
    }
  }

  // Releases a participant context: marks it RELEASED (visible under the lock to a concurrent op
  // that obtained the reference before the removal) and removes it from the map. The caller must
  // hold {@code synchronized(pc)}.
  private void release(String transactionId, ParticipantContext pc) {
    pc.toReleased();
    contexts.remove(transactionId, pc);
  }

  // Resolves the participant context for a known transaction, or throws if none is registered
  // (never joined, already terminated and removed, or expired). Callers must synchronize on the
  // returned context before reading/transitioning its status or touching its TransactionContext.
  private ParticipantContext getParticipantContext(String transactionId)
      throws TransactionNotFoundException {
    ParticipantContext context = contexts.get(transactionId);
    if (context == null) {
      throw new TransactionNotFoundException(
          CoreError.TRANSACTION_NOT_FOUND.buildMessage(), transactionId);
    }
    return context;
  }

  // Rejects a mutation on a read-only transaction. Read-only is carried on the context from join,
  // so the Participant is the authoritative enforcement point (CRUD lands here).
  private void checkWritable(String transactionId, TransactionContext context) {
    if (context.readOnly) {
      throw new IllegalStateException(
          CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(transactionId));
    }
  }

  private void checkMutation(String transactionId, Mutation mutation) throws CrudException {
    try {
      operationChecker.check(mutation);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, transactionId);
    }
  }

  private List<TwoPhaseCommit.WriteSetEntry> buildWriteSetEntries(
      TransactionContext context, TwoPhaseCommit.WriteSetDetailLevel detailLevel) {
    Snapshot snapshot = context.snapshot;
    List<TwoPhaseCommit.WriteSetEntry> entries = new ArrayList<>();
    // Returns empty iff !snapshot.hasWritesOrDeletes(), so the write set shipped to the Coordinator
    // is empty exactly for a write-less participant — regardless of detailLevel, which only
    // controls whether WRITE entries carry non-key columns, never which entries exist. The
    // Coordinator gates writing the COMMITTED state row on this write set being non-empty (its
    // hasWrites), which must agree with isCommitRequired and the validate-step self-release (both
    // derived from context.isCommitRequired(), i.e. snapshot.hasWritesOrDeletes()); preserve this
    // empty-iff-write-less guarantee if this method ever starts filtering entries.
    if (!snapshot.hasWritesOrDeletes()) {
      return entries;
    }
    for (Map.Entry<Snapshot.Key, Put> e : snapshot.getWriteSet()) {
      Put put = e.getValue();
      List<Column<?>> columns;
      switch (detailLevel) {
        case KEYS_ONLY:
          // KEYS_ONLY entries must carry no non-key columns. Skipping the column pass also skips
          // the table-metadata lookup, which is needed only to filter transaction-meta columns.
          columns = Collections.emptyList();
          break;
        case FULL:
          TableMetadata metadata = getTableMetadataForMutation(put);
          List<Column<?>> filteredColumns = new ArrayList<>();
          for (Column<?> column : put.getColumns().values()) {
            if (!ConsensusCommitUtils.isTransactionMetaColumn(column.getName(), metadata)) {
              filteredColumns.add(column);
            }
          }
          columns = Collections.unmodifiableList(filteredColumns);
          break;
        default:
          throw new AssertionError("Unknown write-set detail: " + detailLevel);
      }
      entries.add(
          new WriteSetEntryImpl(
              TwoPhaseCommit.WriteSetEntry.Type.WRITE,
              put.forNamespace().get(),
              put.forTable().get(),
              put.getPartitionKey(),
              put.getClusteringKey(),
              columns));
    }
    for (Map.Entry<Snapshot.Key, Delete> e : snapshot.getDeleteSet()) {
      Delete delete = e.getValue();
      entries.add(
          new WriteSetEntryImpl(
              TwoPhaseCommit.WriteSetEntry.Type.DELETE,
              delete.forNamespace().get(),
              delete.forTable().get(),
              delete.getPartitionKey(),
              delete.getClusteringKey(),
              Collections.emptyList()));
    }
    return entries;
  }

  private TableMetadata getTableMetadataForMutation(Mutation mutation) {
    try {
      return ConsensusCommitUtils.getTransactionTableMetadata(tableMetadataManager, mutation)
          .getTableMetadata();
    } catch (ExecutionException e) {
      throw new AssertionError(
          "Failed to retrieve transaction table metadata while building write set entries."
              + " Operation: "
              + mutation,
          e);
    }
  }

  @Override
  public void close() {
    storage.close();
    admin.close();
    parallelExecutor.close();
    recoveryExecutor.close();
  }

  private static final class PreparationResultImpl implements TwoPhaseCommit.PreparationResult {
    private final List<TwoPhaseCommit.WriteSetEntry> writeSet;
    private final boolean validationRequired;
    private final boolean commitRequired;

    PreparationResultImpl(
        List<TwoPhaseCommit.WriteSetEntry> writeSet,
        boolean validationRequired,
        boolean commitRequired) {
      this.writeSet = writeSet;
      this.validationRequired = validationRequired;
      this.commitRequired = commitRequired;
    }

    @Override
    public List<TwoPhaseCommit.WriteSetEntry> getWriteSet() {
      return writeSet;
    }

    @Override
    public boolean isValidationRequired() {
      return validationRequired;
    }

    @Override
    public boolean isCommitRequired() {
      return commitRequired;
    }
  }

  private static final class WriteSetEntryImpl implements TwoPhaseCommit.WriteSetEntry {
    private final Type type;
    private final String namespaceName;
    private final String tableName;
    private final Key partitionKey;
    private final Optional<Key> clusteringKey;
    private final List<Column<?>> columns;

    WriteSetEntryImpl(
        Type type,
        String namespaceName,
        String tableName,
        Key partitionKey,
        Optional<Key> clusteringKey,
        List<Column<?>> columns) {
      this.type = type;
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.columns = columns;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public String getNamespaceName() {
      return namespaceName;
    }

    @Override
    public String getTableName() {
      return tableName;
    }

    @Override
    public Key getPartitionKey() {
      return partitionKey;
    }

    @Override
    public Optional<Key> getClusteringKey() {
      return clusteringKey;
    }

    @Override
    public List<Column<?>> getColumns() {
      return columns;
    }
  }

  /**
   * A {@link TransactionCrudOperable.Scanner} that serializes its operations on the per-transaction
   * {@link ParticipantContext} — the same monitor every participant method holds, including the
   * context release that closes the scanners ({@code commitRecords} / {@code rollbackRecords}).
   * This keeps scanner iteration and context release mutually exclusive, so the not-thread-safe
   * underlying scanner is never touched by two threads at once.
   *
   * <p>Each {@code one()} / {@code all()} re-checks, under the lock, that the context has not been
   * released; an iteration that loses the race to a release surfaces a retriable {@link
   * CrudConflictException} (the {@link TransactionNotFoundException} the released context would
   * raise cannot be thrown from the scanner API) instead of reading through a released, closed
   * scanner. {@code iterator()} returns an iterator that drives {@code one()} for every element, so
   * {@code hasNext()} / {@code next()} inherit the same lock and liveness re-check.
   */
  private static final class SynchronizedParticipantScanner
      implements TransactionCrudOperable.Scanner {

    private final ParticipantContext pc;
    private final TransactionCrudOperable.Scanner delegate;

    SynchronizedParticipantScanner(
        ParticipantContext pc, TransactionCrudOperable.Scanner delegate) {
      this.pc = pc;
      this.delegate = delegate;
    }

    @Override
    public Optional<Result> one() throws CrudException {
      synchronized (pc) {
        checkNotReleased();
        return delegate.one();
      }
    }

    @Override
    public List<Result> all() throws CrudException {
      synchronized (pc) {
        checkNotReleased();
        return delegate.all();
      }
    }

    @Override
    public void close() throws CrudException {
      synchronized (pc) {
        delegate.close();
      }
    }

    @Nonnull
    @Override
    public Iterator<Result> iterator() {
      // Drive iteration through this wrapper's one() (synchronized on pc, with the liveness
      // re-check) rather than returning delegate.iterator(): the delegate's iterator is bound to
      // the raw scanner, so its hasNext()/next() would touch the underlying scanner without the
      // lock.
      return new ScannerIterator<>(this);
    }

    // Translates a context released by a concurrent terminal step (a commitRecords /
    // rollbackRecords on another thread) into a retriable conflict, so the caller retries the whole
    // transaction rather than iterating a released, closed scanner. Must be called while
    // synchronized on pc.
    private void checkNotReleased() throws CrudConflictException {
      try {
        pc.checkAlive();
      } catch (TransactionNotFoundException e) {
        throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
    }
  }

  /**
   * Per-transaction state: the shared {@link TransactionContext} plus a two-phase-commit lifecycle
   * status. The status enforces the order the Coordinator drives the protocol in (and rejects
   * application operations that arrive out of phase) by throwing {@link IllegalStateException}. It
   * is kept here rather than on {@link TransactionContext} because that class is shared with the
   * single-phase paths ({@code ConsensusCommit} / {@code ConsensusCommitManager}), which have no
   * such lifecycle.
   *
   * <p>The entry is removed from {@link #contexts} at the participant's last driven step (no
   * terminal retention): {@code commitRecords} / {@code rollbackRecords} for a participant with
   * writes, or an earlier step ({@code validateRecords}, or {@code prepareRecords} when validation
   * is not required either) for a write-less participant whose later steps the Coordinator skips.
   * {@link Status#RELEASED} is not for retention: it only lets a concurrent in-flight operation
   * that obtained its reference before the removal observe the termination (under the lock) and
   * fail with {@link TransactionNotFoundException} instead of acting on a finished transaction.
   *
   * <p>Not internally synchronized: every status read/transition below must be performed while
   * synchronized on the instance (the participant holds {@code synchronized(context)} across check,
   * work, and transition so they are atomic and concurrent same-transaction operations are
   * serialized).
   */
  private static final class ParticipantContext {

    private enum Status {
      ACTIVE,
      PREPARED,
      VALIDATED,
      RELEASED
    }

    private final TransactionContext context;
    private Status status = Status.ACTIVE;

    ParticipantContext(TransactionContext context) {
      this.context = context;
    }

    TransactionContext context() {
      return context;
    }

    /** Throws if the transaction has already been terminated (committed or rolled back). */
    void checkAlive() throws TransactionNotFoundException {
      if (status == Status.RELEASED) {
        throw new TransactionNotFoundException(
            CoreError.TRANSACTION_NOT_FOUND.buildMessage(), context.transactionId);
      }
    }

    /** Requires {@code ACTIVE} (CRUD operations and {@code prepareRecords}). */
    void checkActive() throws TransactionNotFoundException {
      checkAlive();
      if (status != Status.ACTIVE) {
        throw new IllegalStateException(CoreError.TRANSACTION_NOT_ACTIVE.buildMessage(status));
      }
    }

    /** Requires {@code PREPARED} ({@code validateRecords}). */
    void checkPrepared() throws TransactionNotFoundException {
      checkAlive();
      if (status != Status.PREPARED) {
        throw new IllegalStateException(CoreError.TRANSACTION_NOT_PREPARED.buildMessage(status));
      }
    }

    /** Requires {@code PREPARED} or {@code VALIDATED} ({@code commitRecords}). */
    void checkPreparedOrValidated() throws TransactionNotFoundException {
      checkAlive();
      if (status != Status.PREPARED && status != Status.VALIDATED) {
        throw new IllegalStateException(
            CoreError.TRANSACTION_NOT_PREPARED_OR_VALIDATED.buildMessage(status));
      }
    }

    /** Whether records have been PREPARED in storage (so a rollback must undo them). */
    boolean isPrepared() {
      return status == Status.PREPARED || status == Status.VALIDATED;
    }

    /** Whether the context has already been released (terminated). */
    boolean isReleased() {
      return status == Status.RELEASED;
    }

    void toPrepared() {
      status = Status.PREPARED;
    }

    void toValidated() {
      status = Status.VALIDATED;
    }

    void toReleased() {
      status = Status.RELEASED;
    }
  }
}
