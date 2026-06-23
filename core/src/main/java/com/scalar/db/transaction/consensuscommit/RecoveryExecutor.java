package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.createAfterImageColumnsFromBeforeImage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RecoveryExecutor implements AutoCloseable {

  // The maximum number of re-resolution passes in resolveLatestResultAndRecover. Each pass is
  // driven by a real concurrent change (a different transaction re-prepared the record, or the
  // writer was resolved while we tried to abort it), so the loop terminates quickly in practice;
  // this is only a backstop against pathological contention.
  @VisibleForTesting static final int MAX_RESOLUTION_ATTEMPTS = 5;

  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final RecoveryHandler recovery;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ExecutorService executorService;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public RecoveryExecutor(
      DistributedStorage storage,
      Coordinator coordinator,
      RecoveryHandler recovery,
      TransactionTableMetadataManager tableMetadataManager) {
    this.storage = Objects.requireNonNull(storage);
    this.coordinator = Objects.requireNonNull(coordinator);
    this.recovery = Objects.requireNonNull(recovery);
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    executorService =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("recovery-executor-%d")
                .setDaemon(true)
                .build());
  }

  /**
   * Resolves the value to return for an uncommitted record encountered during a read and recovers
   * the record asynchronously.
   *
   * <p>The record recovery runs on a background thread, exposed through {@link
   * Result#recoveryFuture}, and delegates to the best-effort {@link
   * RecoveryHandler#tryRecover(Selection, TransactionResult, Optional)}. <b>The record is not
   * guaranteed to be recovered, even after {@link Result#recoveryFuture} completes</b>: a completed
   * future only means the recovery attempt ran, not that the record reached a resolved physical
   * state. Recovery is intentionally skipped when the writer has no coordinator state and has not
   * expired (it may still be in flight), and a roll that conflicts with a concurrent actor is
   * deferred to a subsequent read. This is acceptable on the lazy-recovery read path because a
   * later read retries recovery. Callers that require the record to be resolved before proceeding
   * must use {@link #executeSynchronously(Selection, TransactionResult, Optional)} instead, which
   * guarantees the record is recovered when it returns {@code true}.
   *
   * <p>The {@code recoveryType} controls what value is returned and whether recovery is attempted:
   *
   * <ul>
   *   <li>{@code RETURN_LATEST_RESULT_AND_RECOVER}: read the coordinator state and return the
   *       latest result — the after-image if the writer committed, otherwise the before-image —
   *       then recover the record. Throws {@link UncommittedRecordException} when the writer has no
   *       coordinator state and has not expired (it may still be in flight).
   *   <li>{@code RETURN_COMMITTED_RESULT_AND_RECOVER}: return the committed (before-image) result
   *       immediately and recover the record on the background thread, where the coordinator state
   *       is read and the not-expired guard is applied.
   *   <li>{@code RETURN_COMMITTED_RESULT_AND_NOT_RECOVER}: return the committed (before-image)
   *       result without attempting any recovery (the future completes immediately).
   * </ul>
   *
   * @param key the snapshot key identifying the record
   * @param selection the selection that read the record
   * @param result the latest known uncommitted {@link TransactionResult} for the record
   * @param transactionId the ID of the reading transaction (used for error reporting)
   * @param recoveryType the recovery strategy to apply (see above)
   * @return a {@link Result} holding the value to return, the future that completes when the
   *     background recovery attempt finishes (completion does not imply the record was recovered;
   *     see above), and whether the record was rolled back
   * @throws UncommittedRecordException if the record is uncommitted by a writer that has no
   *     coordinator state and has not expired (it may still be in flight)
   * @throws CrudConflictException if resolving an uncommitted record exceeds the retry limit
   * @throws CrudException if reading the coordinator state or table metadata fails
   */
  public Result execute(
      Snapshot.Key key,
      Selection selection,
      TransactionResult result,
      String transactionId,
      RecoveryType recoveryType)
      throws CrudException {
    assert !result.isCommitted();

    switch (recoveryType) {
      case RETURN_LATEST_RESULT_AND_RECOVER:
        return resolveLatestResultAndRecover(key, selection, result, transactionId);
      case RETURN_COMMITTED_RESULT_AND_RECOVER:
        // Return the committed (before-image) result, and recover the record on the background
        // thread, where the coordinator state is read and the not-expired guard is applied.
        Future<Void> future =
            executorService.submit(
                () -> {
                  Optional<Coordinator.State> s = getCoordinatorState(result.getId());

                  throwUncommittedRecordExceptionIfTransactionNotExpired(
                      s, selection, result, transactionId);

                  recovery.tryRecover(selection, result, s);
                  return null;
                });

        // The rollback/rollforward decision is deferred to the recovery thread, but since we
        // return the committed (before-image) result, we treat this as rolled back.
        return new Result(
            key, createRecordFromBeforeImage(selection, result, transactionId), future, true);
      case RETURN_COMMITTED_RESULT_AND_NOT_RECOVER:
        // Return the committed (before-image) result without attempting any recovery.
        return new Result(
            key,
            createRecordFromBeforeImage(selection, result, transactionId),
            Futures.immediateFuture(null),
            true);
      default:
        throw new AssertionError("Unknown recovery type: " + recoveryType);
    }
  }

  /**
   * Resolves the value to return for an uncommitted record read under {@code
   * RETURN_LATEST_RESULT_AND_RECOVER} and recovers the record.
   *
   * <p>When the writer transaction has a coordinator state, the latest result is returned (after-
   * or before-image) and the record is recovered in the background. When it does not, the
   * transaction may either still be in flight (not expired — the read is blocked with {@link
   * UncommittedRecordException}) or have left a stale record. Because a Coordinator state cleanup
   * process can run, an absent state no longer implies the transaction never committed, so the
   * record is physically re-read to decide:
   *
   * <ul>
   *   <li>record gone — a committed delete or rolled-back insert: return empty.
   *   <li>record committed — the transaction committed and was cleaned up: return its value.
   *   <li>record re-prepared by a different transaction: re-resolve against that transaction.
   *   <li>record still uncommitted by the same expired transaction: it is genuinely dead, so abort
   *       it (write its ABORTED coordinator state) and, only once that succeeds, return the
   *       before-image and roll the record back in the background. If the abort conflicts (the
   *       transaction was concurrently resolved), re-resolve from the top.
   * </ul>
   *
   * The re-resolution loop is bounded by {@link #MAX_RESOLUTION_ATTEMPTS}.
   */
  private Result resolveLatestResultAndRecover(
      Snapshot.Key key, Selection selection, TransactionResult result, String transactionId)
      throws CrudException {
    TransactionResult current = result;
    for (int attempt = 0; ; attempt++) {
      // current is always an uncommitted record, so its tx_id is non-null here; getId() is
      // @Nullable only for deemed-committed records, which are never recovered.
      String txId = current.getId();
      assert txId != null;

      Optional<Coordinator.State> state = getCoordinatorState(txId);

      if (state.isPresent()) {
        boolean rolledBack = state.get().getState() == TransactionState.ABORTED;
        Optional<TransactionResult> recoveredResult =
            createRecoveredResult(state.get(), selection, current, transactionId);
        TransactionResult recovered = current;
        Future<Void> future =
            executorService.submit(
                () -> {
                  recovery.tryRecover(selection, recovered, state);
                  return null;
                });
        return new Result(key, recoveredResult, future, rolledBack);
      }

      // The coordinator state is absent. Throw if the transaction has not expired (it may still be
      // in flight); otherwise it has expired and must be aborted.
      throwUncommittedRecordExceptionIfTransactionNotExpired(
          state, selection, current, transactionId);

      // The transaction has expired with no coordinator state. Re-read the record physically:
      // because a Coordinator state cleanup process can run, the previously observed uncommitted
      // result may be stale.
      Optional<TransactionResult> fresh;
      try {
        fresh =
            ConsensusCommitUtils.rereadRecord(storage, tableMetadataManager, selection, current);
      } catch (ExecutionException e) {
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
            e,
            transactionId);
      }

      if (!fresh.isPresent()) {
        // The record is gone (a committed delete, or a rolled-back insert). Return empty.
        return new Result(key, Optional.empty(), Futures.immediateFuture(null), true);
      }
      if (fresh.get().isCommitted()) {
        // The transaction committed and was cleaned up; the record carries the committed value.
        return new Result(key, fresh, Futures.immediateFuture(null), false);
      }
      if (!txId.equals(fresh.get().getId())) {
        // A different transaction re-prepared the record. Re-resolve against it.
        checkReadRetryLimit(attempt, transactionId);
        current = fresh.get();
        continue;
      }

      // The record is still uncommitted by the same expired transaction: it is genuinely dead. The
      // before-image is only the correct value to return once that transaction is confirmed
      // aborted, so abort it synchronously (write its ABORTED coordinator state) first.
      boolean aborted;
      try {
        aborted = recovery.tryAbortExpiredTransaction(txId);
      } catch (CoordinatorException e) {
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
            e,
            transactionId);
      }

      if (aborted) {
        // The ABORTED coordinator state was written with putIfNotExists, which also succeeds when
        // the row is absent. So the writer may actually have committed, been finalized, and had its
        // COMMITTED state removed by a Coordinator state cleanup process in the window between the
        // pre-abort re-read above and this abort. In that race the ABORTED we just wrote is
        // spurious and the record carries the committed value, so re-read once more and return the
        // record's actual state instead of a possibly stale before-image. (The spurious ABORTED is
        // non-corrupting -- the record stays committed and the background rollback below is a
        // conditional no-op -- and is reclaimed by the Coordinator state cleanup process.)
        Optional<TransactionResult> afterAbort;
        try {
          afterAbort =
              ConsensusCommitUtils.rereadRecord(storage, tableMetadataManager, selection, current);
        } catch (ExecutionException e) {
          throw new CrudException(
              CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
              e,
              transactionId);
        }
        if (!afterAbort.isPresent()) {
          // The record is gone (a committed delete that raced the cleanup). Return empty.
          return new Result(key, Optional.empty(), Futures.immediateFuture(null), true);
        }
        if (afterAbort.get().isCommitted()) {
          // The writer committed and was cleaned up; the record carries the committed value.
          return new Result(key, afterAbort, Futures.immediateFuture(null), false);
        }
        if (!txId.equals(afterAbort.get().getId())) {
          // A different transaction re-prepared the record after we aborted the writer (the writer
          // was rolled back and, possibly through one or more intervening commits, a new writer
          // re-prepared it). current's before-image is no longer guaranteed to be the latest
          // committed value, so re-resolve against the new writer instead of returning a stale
          // before-image -- the same ABA handling as the pre-abort re-read above.
          checkReadRetryLimit(attempt, transactionId);
          current = afterAbort.get();
          continue;
        }

        // The record is still uncommitted by the same expired writer (the genuine-abort case): the
        // writer is now aborted, so return the before-image and roll the record back in the
        // background. The before-image is built from current (the pre-abort snapshot), not
        // afterAbort: only a coordinator write happened between the two physical reads, so the
        // record (and thus its before-image) is unchanged, and the branches above already ruled out
        // the record having been committed, removed, or re-prepared by a different transaction.
        Optional<TransactionResult> recoveredResult =
            createRecordFromBeforeImage(selection, current, transactionId);
        TransactionResult recovered = current;
        Future<Void> future =
            executorService.submit(
                () -> {
                  recovery.rollbackRecord(selection, recovered);
                  return null;
                });
        return new Result(key, recoveredResult, future, true);
      }

      // Writing the ABORTED state conflicted: a concurrent actor resolved the transaction (e.g. it
      // committed) or a cleanup raced. Re-resolve from the top.
      checkReadRetryLimit(attempt, transactionId);
    }
  }

  private void checkReadRetryLimit(int attempt, String transactionId) throws CrudConflictException {
    // attempt is the current 0-based pass index, checked at the end of a pass before retrying, so
    // the last allowed pass is MAX_RESOLUTION_ATTEMPTS - 1.
    if (attempt >= MAX_RESOLUTION_ATTEMPTS - 1) {
      throw new CrudConflictException(
          CoreError.CONSENSUS_COMMIT_RESOLVING_UNCOMMITTED_RECORD_RETRY_LIMIT_EXCEEDED.buildMessage(
              transactionId),
          transactionId);
    }
  }

  private Optional<Coordinator.State> getCoordinatorState(String transactionId)
      throws CrudException {
    try {
      return coordinator.getState(transactionId);
    } catch (CoordinatorException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          transactionId);
    }
  }

  private Optional<TransactionResult> createRecoveredResult(
      Coordinator.State state, Selection selection, TransactionResult result, String transactionId)
      throws CrudException {
    if (state.getState() == TransactionState.ABORTED) {
      return createRecordFromBeforeImage(selection, result, transactionId);
    }
    assert state.getState() == TransactionState.COMMITTED;
    return createResultFromAfterImage(selection, result, transactionId);
  }

  private void throwUncommittedRecordExceptionIfTransactionNotExpired(
      Optional<Coordinator.State> state,
      Selection selection,
      TransactionResult result,
      String transactionId)
      throws CrudException {
    assert selection.forFullTableName().isPresent();

    if (!state.isPresent() && !recovery.isTransactionExpired(result)) {
      TransactionTableMetadata transactionTableMetadata =
          getTransactionTableMetadata(selection, transactionId);
      throw new UncommittedRecordException(
          selection,
          result,
          CoreError.CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD.buildMessage(
              selection.forFullTableName().get(),
              ScalarDbUtils.getPartitionKey(result, transactionTableMetadata.getTableMetadata()),
              ScalarDbUtils.getClusteringKey(result, transactionTableMetadata.getTableMetadata()),
              result.getId()),
          transactionId);
    }
  }

  private Optional<TransactionResult> createRecordFromBeforeImage(
      Selection selection, TransactionResult result, String transactionId) throws CrudException {
    if (!result.hasBeforeImage()) {
      return Optional.empty();
    }

    TransactionTableMetadata transactionTableMetadata =
        getTransactionTableMetadata(selection, transactionId);
    LinkedHashSet<String> beforeImageColumnNames =
        transactionTableMetadata.getBeforeImageColumnNames();
    TableMetadata tableMetadata = transactionTableMetadata.getTableMetadata();

    Map<String, Column<?>> columns = new HashMap<>();

    createAfterImageColumnsFromBeforeImage(columns, result, beforeImageColumnNames);

    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata);
    partitionKey.getColumns().forEach(c -> columns.put(c.getName(), c));

    Optional<Key> clusteringKey = ScalarDbUtils.getClusteringKey(result, tableMetadata);
    clusteringKey.ifPresent(k -> k.getColumns().forEach(c -> columns.put(c.getName(), c)));

    addNullBeforeImageColumns(columns, beforeImageColumnNames, tableMetadata);

    return Optional.of(new TransactionResult(new ResultImpl(columns, tableMetadata)));
  }

  private Optional<TransactionResult> createResultFromAfterImage(
      Selection selection, TransactionResult result, String transactionId) throws CrudException {
    if (result.getState() == TransactionState.DELETED) {
      return Optional.empty();
    }

    assert result.getState() == TransactionState.PREPARED;

    TransactionTableMetadata transactionTableMetadata =
        getTransactionTableMetadata(selection, transactionId);
    TableMetadata tableMetadata = transactionTableMetadata.getTableMetadata();

    Map<String, Column<?>> columns = new HashMap<>();
    result
        .getColumns()
        .forEach(
            (columnName, column) -> {
              if (columnName.equals(Attribute.STATE)) {
                // Set the state to COMMITTED
                columns.put(
                    Attribute.STATE,
                    IntColumn.of(Attribute.STATE, TransactionState.COMMITTED.get()));
              } else {
                columns.put(columnName, column);
              }
            });

    long committedAt = getCommittedAt();
    columns.put(Attribute.COMMITTED_AT, BigIntColumn.of(Attribute.COMMITTED_AT, committedAt));

    addNullBeforeImageColumns(
        columns, transactionTableMetadata.getBeforeImageColumnNames(), tableMetadata);

    return Optional.of(new TransactionResult(new ResultImpl(columns, tableMetadata)));
  }

  @VisibleForTesting
  long getCommittedAt() {
    // Use the current time as the committedAt timestamp. Note that this is not the actual
    // committedAt timestamp of the record
    return System.currentTimeMillis();
  }

  private void addNullBeforeImageColumns(
      Map<String, Column<?>> columns,
      LinkedHashSet<String> beforeImageColumnNames,
      TableMetadata tableMetadata) {
    for (String beforeImageColumnName : beforeImageColumnNames) {
      DataType columnDataType = tableMetadata.getColumnDataType(beforeImageColumnName);
      switch (columnDataType) {
        case BOOLEAN:
          columns.put(beforeImageColumnName, BooleanColumn.ofNull(beforeImageColumnName));
          break;
        case INT:
          columns.put(beforeImageColumnName, IntColumn.ofNull(beforeImageColumnName));
          break;
        case BIGINT:
          columns.put(beforeImageColumnName, BigIntColumn.ofNull(beforeImageColumnName));
          break;
        case FLOAT:
          columns.put(beforeImageColumnName, FloatColumn.ofNull(beforeImageColumnName));
          break;
        case DOUBLE:
          columns.put(beforeImageColumnName, DoubleColumn.ofNull(beforeImageColumnName));
          break;
        case TEXT:
          columns.put(beforeImageColumnName, TextColumn.ofNull(beforeImageColumnName));
          break;
        case BLOB:
          columns.put(beforeImageColumnName, BlobColumn.ofNull(beforeImageColumnName));
          break;
        case DATE:
          columns.put(beforeImageColumnName, DateColumn.ofNull(beforeImageColumnName));
          break;
        case TIME:
          columns.put(beforeImageColumnName, TimeColumn.ofNull(beforeImageColumnName));
          break;
        case TIMESTAMP:
          columns.put(beforeImageColumnName, TimestampColumn.ofNull(beforeImageColumnName));
          break;
        case TIMESTAMPTZ:
          columns.put(beforeImageColumnName, TimestampTZColumn.ofNull(beforeImageColumnName));
          break;
        default:
          throw new AssertionError("Unknown data type: " + columnDataType);
      }
    }
  }

  private TransactionTableMetadata getTransactionTableMetadata(
      Operation operation, String transactionId) throws CrudException {
    assert operation.forFullTableName().isPresent();

    try {
      return ConsensusCommitUtils.getTransactionTableMetadata(tableMetadataManager, operation);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(operation.forFullTableName().get()),
          e,
          transactionId);
    }
  }

  /**
   * Recovers a single record synchronously when the coordinator state is already known to be
   * present. This is the entry point for callers that always hold a terminal coordinator state.
   *
   * <p>It delegates to {@link RecoveryHandler#recover(Selection, TransactionResult,
   * Coordinator.State)}, the present-state overload that always rolls the record and never touches
   * the coordinator — so the record is guaranteed to be recovered and {@link CoordinatorException}
   * cannot be thrown. That overload returns nothing, so this method does too.
   *
   * @param selection the selection that identifies the user record
   * @param result the latest known TransactionResult for the record
   * @param state the coordinator state for the transaction that wrote the record
   * @throws ExecutionException if the underlying storage read or mutation fails
   */
  void executeSynchronously(Selection selection, TransactionResult result, Coordinator.State state)
      throws ExecutionException {
    recovery.recover(selection, result, state);
  }

  /**
   * Recovers a single record synchronously, blocking until the recovery completes, and reports
   * whether the record was recovered.
   *
   * <p>This delegates to {@link RecoveryHandler#recover(Selection, TransactionResult, Optional)},
   * which guarantees that when {@code true} is returned the record has been physically resolved —
   * the entry point for the {@code recoverRecord} API. See that method for the full semantics and
   * the meaning of the returned value.
   *
   * @param selection the selection that identifies the user record
   * @param result the latest known TransactionResult for the record
   * @param state the coordinator state for the transaction that wrote the record, if any
   * @return {@code true} if the record was recovered (resolved to a terminal state), {@code false}
   *     if the writer may still be in flight and the call should be retried later
   * @throws ExecutionException if the underlying storage read or mutation fails
   * @throws CoordinatorException if reading or updating the coordinator state fails
   */
  boolean executeSynchronously(
      Selection selection, TransactionResult result, Optional<Coordinator.State> state)
      throws ExecutionException, CoordinatorException {
    return recovery.recover(selection, result, state);
  }

  @Override
  public void close() {
    executorService.shutdown();
    Uninterruptibles.awaitTerminationUninterruptibly(executorService);
  }

  public static class Result {
    public final Snapshot.Key key;

    // The recovered result
    public final Optional<TransactionResult> recoveredResult;

    // The future that completes when the recovery is done
    public final Future<Void> recoveryFuture;

    // Whether the record was rolled back (true) or rolled forward (false)
    public final boolean rolledBack;

    public Result(
        Snapshot.Key key,
        Optional<TransactionResult> recoveredResult,
        Future<Void> recoveryFuture,
        boolean rolledBack) {
      this.key = key;
      this.recoveredResult = recoveredResult;
      this.recoveryFuture = recoveryFuture;
      this.rolledBack = rolledBack;
    }
  }

  public enum RecoveryType {
    RETURN_LATEST_RESULT_AND_RECOVER,
    RETURN_COMMITTED_RESULT_AND_RECOVER,
    RETURN_COMMITTED_RESULT_AND_NOT_RECOVER
  }
}
