package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.extractAfterImageColumnsFromBeforeImage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
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

  private final Coordinator coordinator;
  private final RecoveryHandler recovery;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ExecutorService executorService;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public RecoveryExecutor(
      Coordinator coordinator,
      RecoveryHandler recovery,
      TransactionTableMetadataManager tableMetadataManager,
      int threadPoolSize) {
    this.coordinator = Objects.requireNonNull(coordinator);
    this.recovery = Objects.requireNonNull(recovery);
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    executorService =
        Executors.newFixedThreadPool(
            threadPoolSize,
            new ThreadFactoryBuilder()
                .setNameFormat("recovery-executor-%d")
                .setDaemon(true)
                .build());
  }

  public Result execute(
      Snapshot.Key key, Selection selection, TransactionResult result, String transactionId)
      throws CrudException {
    assert !result.isCommitted();

    Optional<Coordinator.State> state = getCoordinatorState(result.getId());

    Optional<TransactionResult> recoveredResult =
        createRecoveredResult(state, selection, result, transactionId);

    // Recover the record
    Future<Void> future =
        executorService.submit(
            () -> {
              recovery.recover(selection, result, state);
              return null;
            });

    return new Result(key, recoveredResult, future);
  }

  private Optional<Coordinator.State> getCoordinatorState(String transactionId)
      throws CrudException {
    try {
      return coordinator.getState(transactionId);
    } catch (CoordinatorException e) {
      throw new CrudException(e.getMessage(), e, transactionId);
    }
  }

  private Optional<TransactionResult> createRecoveredResult(
      Optional<Coordinator.State> state,
      Selection selection,
      TransactionResult result,
      String transactionId)
      throws CrudException {
    throwUncommittedRecordExceptionIfTransactionNotExpired(state, selection, result, transactionId);

    if (!state.isPresent() || state.get().getState() == TransactionState.ABORTED) {
      return createRolledBackRecord(selection, result, transactionId);
    } else {
      assert state.get().getState() == TransactionState.COMMITTED;
      return createRolledForwardResult(selection, result, transactionId);
    }
  }

  private void throwUncommittedRecordExceptionIfTransactionNotExpired(
      Optional<Coordinator.State> state,
      Selection selection,
      TransactionResult result,
      String transactionId)
      throws UncommittedRecordException {
    if (!state.isPresent() && !recovery.isTransactionExpired(result)) {
      throw new UncommittedRecordException(
          selection,
          result,
          CoreError.CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD.buildMessage(),
          transactionId);
    }
  }

  private Optional<TransactionResult> createRolledBackRecord(
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

    extractAfterImageColumnsFromBeforeImage(columns, result, beforeImageColumnNames);

    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata);
    partitionKey.getColumns().forEach(c -> columns.put(c.getName(), c));

    Optional<Key> clusteringKey = ScalarDbUtils.getClusteringKey(result, tableMetadata);
    clusteringKey.ifPresent(k -> k.getColumns().forEach(c -> columns.put(c.getName(), c)));

    addNullBeforeImageColumns(columns, beforeImageColumnNames, tableMetadata);

    return Optional.of(new TransactionResult(new ResultImpl(columns, tableMetadata)));
  }

  private Optional<TransactionResult> createRolledForwardResult(
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
    try {
      return ConsensusCommitUtils.getTransactionTableMetadata(tableMetadataManager, operation);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(), e, transactionId);
    }
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

    public Result(
        Snapshot.Key key,
        Optional<TransactionResult> recoveredResult,
        Future<Void> recoveryFuture) {
      this.key = key;
      this.recoveredResult = recoveredResult;
      this.recoveryFuture = recoveryFuture;
    }
  }
}
