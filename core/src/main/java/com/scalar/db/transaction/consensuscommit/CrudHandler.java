package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes.isImplicitPreReadEnabled;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.AndConditionSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionSetBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.common.AbstractTransactionCrudOperableScanner;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class CrudHandler {
  private static final Logger logger = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final Snapshot snapshot;
  private final RecoveryExecutor recoveryExecutor;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final boolean isIncludeMetadataEnabled;
  private final MutationConditionsValidator mutationConditionsValidator;
  private final ParallelExecutor parallelExecutor;

  // Whether the transaction is in read-only mode or not.
  private final boolean readOnly;

  // Whether the transaction is in one-operation mode or not. One-operation mode refers to executing
  // a CRUD operation directly through `DistributedTransactionManager` without explicitly beginning
  // a transaction.
  private final boolean oneOperation;

  private final List<ConsensusCommitScanner> scanners = new ArrayList<>();
  private final List<RecoveryExecutor.Result> recoveryResults = new ArrayList<>();

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      RecoveryExecutor recoveryExecutor,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      ParallelExecutor parallelExecutor,
      boolean readOnly,
      boolean oneOperation) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = new MutationConditionsValidator(snapshot.getId());
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.readOnly = readOnly;
    this.oneOperation = oneOperation;
  }

  @VisibleForTesting
  CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      RecoveryExecutor recoveryExecutor,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      MutationConditionsValidator mutationConditionsValidator,
      ParallelExecutor parallelExecutor,
      boolean readOnly,
      boolean oneOperation) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = checkNotNull(mutationConditionsValidator);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.readOnly = readOnly;
    this.oneOperation = oneOperation;
  }

  public Optional<Result> get(Get originalGet) throws CrudException {
    List<String> originalProjections = new ArrayList<>(originalGet.getProjections());
    Get get = (Get) prepareStorageSelection(originalGet);

    TableMetadata metadata = getTableMetadata(get);

    Snapshot.Key key;
    if (ScalarDbUtils.isSecondaryIndexSpecified(get, metadata)) {
      // In case of a Get with index, we don't know the key until we read the record
      key = null;
    } else {
      key = new Snapshot.Key(get);
    }

    if (isSnapshotReadRequired()) {
      readUnread(key, get);
      return snapshot
          .getResult(key, get)
          .map(r -> new FilteredResult(r, originalProjections, metadata, isIncludeMetadataEnabled));
    } else {
      Optional<TransactionResult> result = read(key, get);
      return snapshot
          .mergeResult(key, result, get.getConjunctions())
          .map(r -> new FilteredResult(r, originalProjections, metadata, isIncludeMetadataEnabled));
    }
  }

  // Only for a Get with index, the argument `key` is null
  @VisibleForTesting
  void readUnread(@Nullable Snapshot.Key key, Get get) throws CrudException {
    if (!snapshot.containsKeyInGetSet(get)) {
      read(key, get);
    }
  }

  // Although this class is not thread-safe, this method is actually thread-safe, so we call it
  // concurrently in the implicit pre-read
  @VisibleForTesting
  Optional<TransactionResult> read(@Nullable Snapshot.Key key, Get get) throws CrudException {
    Optional<TransactionResult> result = getFromStorage(get);
    if (result.isPresent() && !result.get().isCommitted()) {
      // Lazy recovery

      if (key == null) {
        // Only for a Get with index, the argument `key` is null. In that case, create a key from
        // the result
        TableMetadata tableMetadata = getTableMetadata(get);
        key = new Snapshot.Key(get, result.get(), tableMetadata);
      }

      result = executeRecovery(key, get, result.get());
    }

    if (!get.getConjunctions().isEmpty()) {
      // Because we also get records whose before images match the conjunctions, we need to check if
      // the current status of the records actually match the conjunctions.
      result =
          result.filter(
              r ->
                  ScalarDbUtils.columnsMatchAnyOfConjunctions(
                      r.getColumns(), get.getConjunctions()));
    }

    if (result.isPresent() || get.getConjunctions().isEmpty()) {
      // We put the result into the read set only if a get operation has no conjunction or the
      // result exists. This is because we don’t know whether the record actually exists or not
      // due to the conjunction.

      if (key != null) {
        putIntoReadSetInSnapshot(key, result);
      } else {
        // Only for a Get with index, the argument `key` is null

        if (result.isPresent()) {
          // Only when we can get the record with the Get with index, we can put it into the read
          // set
          TableMetadata tableMetadata = getTableMetadata(get);
          key = new Snapshot.Key(get, result.get(), tableMetadata);
          putIntoReadSetInSnapshot(key, result);
        }
      }
    }
    putIntoGetSetInSnapshot(get, result);
    return result;
  }

  private Optional<TransactionResult> executeRecovery(
      Snapshot.Key key, Selection selection, TransactionResult result) throws CrudException {
    RecoveryExecutor.RecoveryType recoveryType;
    if (snapshot.getIsolation() == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      if (readOnly) {
        // In read-only mode, we don't recover the record, but return the committed result
        recoveryType = RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER;
      } else {
        // In read-write mode, we recover the record and return the committed result
        recoveryType = RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER;
      }
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation, we always recover the record and return the latest
      // result
      recoveryType = RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER;
    }

    RecoveryExecutor.Result recoveryResult =
        recoveryExecutor.execute(key, selection, result, snapshot.getId(), recoveryType);

    recoveryResults.add(recoveryResult);
    return recoveryResult.recoveredResult;
  }

  public List<Result> scan(Scan originalScan) throws CrudException {
    List<String> originalProjections = new ArrayList<>(originalScan.getProjections());
    Scan scan = (Scan) prepareStorageSelection(originalScan);
    LinkedHashMap<Snapshot.Key, TransactionResult> results = scanInternal(scan);
    verifyNoOverlap(scan, results);

    TableMetadata metadata = getTableMetadata(scan);
    return results.values().stream()
        .map(r -> new FilteredResult(r, originalProjections, metadata, isIncludeMetadataEnabled))
        .collect(Collectors.toList());
  }

  private LinkedHashMap<Snapshot.Key, TransactionResult> scanInternal(Scan scan)
      throws CrudException {
    Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> resultsInSnapshot =
        snapshot.getResults(scan);
    if (resultsInSnapshot.isPresent()) {
      return resultsInSnapshot.get();
    }

    LinkedHashMap<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();

    Scanner scanner = null;
    try {
      if (scan.getLimit() > 0) {
        // Since recovery and conjunctions may delete some records from the scan result, it is
        // necessary to perform the scan without a limit.
        scanner = scanFromStorage(Scan.newBuilder(scan).limit(0).build());
      } else {
        scanner = scanFromStorage(scan);
      }

      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        TableMetadata tableMetadata = getTableMetadata(scan);
        Snapshot.Key key = new Snapshot.Key(scan, r, tableMetadata);
        Optional<TransactionResult> processedScanResult = processScanResult(key, scan, result);
        processedScanResult.ifPresent(res -> results.put(key, res));

        if (scan.getLimit() > 0 && results.size() >= scan.getLimit()) {
          // If the scan has a limit, we stop scanning when we reach the limit.
          break;
        }
      }
    } catch (RuntimeException e) {
      Exception exception;
      if (e.getCause() instanceof ExecutionException) {
        exception = (ExecutionException) e.getCause();
      } else {
        exception = e;
      }
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(
              exception.getMessage()),
          exception,
          snapshot.getId());
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          logger.warn("Failed to close the scanner. Transaction ID: {}", snapshot.getId(), e);
        }
      }
    }

    putIntoScanSetInSnapshot(scan, results);

    return results;
  }

  private Optional<TransactionResult> processScanResult(
      Snapshot.Key key, Scan scan, TransactionResult result) throws CrudException {
    Optional<TransactionResult> ret;
    if (!result.isCommitted()) {
      // Lazy recovery
      ret = executeRecovery(key, scan, result);
    } else {
      ret = Optional.of(result);
    }

    if (!scan.getConjunctions().isEmpty()) {
      // Because we also get records whose before images match the conjunctions, we need to check if
      // the current status of the records actually match the conjunctions.
      ret =
          ret.filter(
              r ->
                  ScalarDbUtils.columnsMatchAnyOfConjunctions(
                      r.getColumns(), scan.getConjunctions()));
    }

    if (ret.isPresent()) {
      putIntoReadSetInSnapshot(key, ret);
    }

    return ret;
  }

  public TransactionCrudOperable.Scanner getScanner(Scan originalScan) throws CrudException {
    List<String> originalProjections = new ArrayList<>(originalScan.getProjections());
    Scan scan = (Scan) prepareStorageSelection(originalScan);

    ConsensusCommitScanner scanner;

    Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> resultsInSnapshot =
        snapshot.getResults(scan);
    if (resultsInSnapshot.isPresent()) {
      scanner =
          new ConsensusCommitSnapshotScanner(scan, originalProjections, resultsInSnapshot.get());
    } else {
      scanner = new ConsensusCommitStorageScanner(scan, originalProjections);
    }

    scanners.add(scanner);
    return scanner;
  }

  public boolean areAllScannersClosed() {
    return scanners.stream().allMatch(ConsensusCommitScanner::isClosed);
  }

  public void closeScanners() throws CrudException {
    for (ConsensusCommitScanner scanner : scanners) {
      if (!scanner.isClosed()) {
        scanner.close();
      }
    }
  }

  private void putIntoReadSetInSnapshot(Snapshot.Key key, Optional<TransactionResult> result) {
    // In read-only mode, we don't need to put the result into the read set
    if (!readOnly && !snapshot.containsKeyInReadSet(key)) {
      snapshot.putIntoReadSet(key, result);
    }
  }

  private boolean isSnapshotReadRequired() {
    // In one-operation mode, we don't need snapshot reads
    return !oneOperation && snapshot.isSnapshotReadRequired();
  }

  private boolean isValidationOrSnapshotReadRequired() {
    return snapshot.isValidationRequired() || isSnapshotReadRequired();
  }

  private void putIntoGetSetInSnapshot(Get get, Optional<TransactionResult> result) {
    // If neither validation nor snapshot reads are required, we don't need to put the result into
    // the get set
    if (isValidationOrSnapshotReadRequired()) {
      snapshot.putIntoGetSet(get, result);
    }
  }

  private void putIntoScanSetInSnapshot(
      Scan scan, LinkedHashMap<Snapshot.Key, TransactionResult> results) {
    // If neither validation nor snapshot reads are required, we don't need to put the results into
    // the scan set
    if (isValidationOrSnapshotReadRequired()) {
      snapshot.putIntoScanSet(scan, results);
    }
  }

  private void putIntoScannerSetInSnapshot(
      Scan scan, LinkedHashMap<Snapshot.Key, TransactionResult> results) {
    // if validation is not required, we don't need to put the results into the scanner set
    if (snapshot.isValidationRequired()) {
      snapshot.putIntoScannerSet(scan, results);
    }
  }

  private void verifyNoOverlap(Scan scan, Map<Snapshot.Key, TransactionResult> results) {
    if (isOverlapVerificationRequired()) {
      snapshot.verifyNoOverlap(scan, results);
    }
  }

  private boolean isOverlapVerificationRequired() {
    // In either read-only mode or one-operation mode, we don't need to verify overlap
    return !readOnly && !oneOperation;
  }

  public void put(Put put) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(put);

    if (put.getCondition().isPresent()
        && (!isImplicitPreReadEnabled(put) && !snapshot.containsKeyInReadSet(key))) {
      throw new IllegalArgumentException(
          CoreError
              .CONSENSUS_COMMIT_PUT_CANNOT_HAVE_CONDITION_WHEN_TARGET_RECORD_UNREAD_AND_IMPLICIT_PRE_READ_DISABLED
              .buildMessage(put));
    }

    if (put.getCondition().isPresent()) {
      if (isImplicitPreReadEnabled(put) && !snapshot.containsKeyInReadSet(key)) {
        read(key, createGet(key));
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          put, snapshot.getResult(key).orElse(null));
    }

    snapshot.putIntoWriteSet(key, put);
  }

  public void delete(Delete delete) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(delete);

    if (delete.getCondition().isPresent()) {
      if (!snapshot.containsKeyInReadSet(key)) {
        read(key, createGet(key));
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          delete, snapshot.getResult(key).orElse(null));
    }

    snapshot.putIntoDeleteSet(key, delete);
  }

  public void readIfImplicitPreReadEnabled() throws CrudException {
    List<ParallelExecutor.ParallelExecutorTask> tasks = new ArrayList<>();

    // For each put in the write set, if implicit pre-read is enabled and the record is not read
    // yet, read the record
    for (Put put : snapshot.getPutsInWriteSet()) {
      if (isImplicitPreReadEnabled(put)) {
        Snapshot.Key key = new Snapshot.Key(put);
        if (!snapshot.containsKeyInReadSet(key)) {
          tasks.add(() -> read(key, createGet(key)));
        }
      }
    }

    // For each delete in the write set, if the record is not read yet, read the record
    for (Delete delete : snapshot.getDeletesInDeleteSet()) {
      Snapshot.Key key = new Snapshot.Key(delete);
      if (!snapshot.containsKeyInReadSet(key)) {
        tasks.add(() -> read(key, createGet(key)));
      }
    }

    if (!tasks.isEmpty()) {
      parallelExecutor.executeImplicitPreRead(tasks, snapshot.getId());
    }
  }

  private Get createGet(Snapshot.Key key) {
    GetBuilder.BuildableGet buildableGet =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey());
    key.getClusteringKey().ifPresent(buildableGet::clusteringKey);
    return (Get) prepareStorageSelection(buildableGet.build());
  }

  /**
   * Waits for the completion of recovery tasks if necessary.
   *
   * <p>This method is expected to be called before committing the transaction.
   *
   * <p>We wait for the completion of recovery tasks when the recovered records are either in the
   * write set or delete set, or when serializable validation is required.
   *
   * <p>This is necessary because:
   *
   * <ul>
   *   <li>For records in the write set or delete set, if we don’t wait for recovery tasks for them
   *       to complete, we might attempt to perform prepare-records on records whose status is still
   *       PREPARED or DELETED.
   *       <ul>
   *         <li>If we perform prepare-records on records that should be rolled forward, the
   *             prepare-records will succeed. However, it will create a PREPARED-state before
   *             image, which is unexpected. While this may not affect correctness, it’s something
   *             we should avoid.
   *         <li>If we perform prepare-records on records that should be rolled back, the
   *             prepare-records will always fail, causing the transaction to abort.
   *       </ul>
   *   <li>When serializable validation is required, if we don’t wait for recovery tasks to
   *       complete, the validation could fail due to records with PREPARED or DELETED status.
   * </ul>
   *
   * @throws CrudConflictException if any recovery task fails due to a conflict
   * @throws CrudException if any recovery task fails
   */
  public void waitForRecoveryCompletionIfNecessary() throws CrudException {
    for (RecoveryExecutor.Result recoveryResult : recoveryResults) {
      try {
        if (snapshot.containsKeyInWriteSet(recoveryResult.key)
            || snapshot.containsKeyInDeleteSet(recoveryResult.key)
            || snapshot.isValidationRequired()) {
          recoveryResult.recoveryFuture.get();
        }
      } catch (java.util.concurrent.ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof CrudException) {
          throw (CrudException) cause;
        }

        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(cause.getMessage()),
            cause,
            snapshot.getId());
      } catch (Exception e) {
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
            e,
            snapshot.getId());
      }
    }
  }

  @VisibleForTesting
  void waitForRecoveryCompletion() throws CrudException {
    for (RecoveryExecutor.Result recoveryResult : recoveryResults) {
      try {
        recoveryResult.recoveryFuture.get();
      } catch (java.util.concurrent.ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof CrudException) {
          throw (CrudException) cause;
        }

        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(cause.getMessage()),
            cause,
            snapshot.getId());
      } catch (Exception e) {
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
            e,
            snapshot.getId());
      }
    }
  }

  // Although this class is not thread-safe, this method is actually thread-safe because the storage
  // is thread-safe
  @VisibleForTesting
  Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      if (get.getConjunctions().isEmpty()) {
        // If there are no conjunctions, we can read the record directly
        return storage.get(get).map(TransactionResult::new);
      } else {
        // If there are conjunctions, we need to convert them to include conditions on the before
        // image
        Set<AndConditionSet> converted = convertConjunctions(get, get.getConjunctions());
        Get convertedGet = Get.newBuilder(get).clearConditions().whereOr(converted).build();
        return storage.get(convertedGet).map(TransactionResult::new);
      }
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_READING_RECORD_FROM_STORAGE_FAILED.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  private Scanner scanFromStorage(Scan scan) throws CrudException {
    try {
      if (scan.getConjunctions().isEmpty()) {
        // If there are no conjunctions, we can read the record directly
        return storage.scan(scan);
      } else {
        // If there are conjunctions, we need to convert them to include conditions on the before
        // image
        Set<AndConditionSet> converted = convertConjunctions(scan, scan.getConjunctions());
        Scan convertedScan = Scan.newBuilder(scan).clearConditions().whereOr(converted).build();
        return storage.scan(convertedScan);
      }
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  /**
   * Converts the given conjunctions to include conditions on before images.
   *
   * <p>This is necessary because we might miss prepared records whose before images match the
   * original conditions when reading from storage. For example, suppose we have the following
   * records in storage:
   *
   * <pre>
   *   | partition_key | clustering_key | column | status    | before_column | before_status  |
   *   |---------------|----------------|--------|-----------|---------------|----------------|
   *   | 0             | 0              | 1000   | COMMITTED |               |                |
   *   | 0             | 1              | 200    | PREPARED  | 1000          | COMMITTED      |
   * </pre>
   *
   * If we scan records with the condition "column = 1000" without converting the condition
   * (conjunction), we only get the first record, not the second one, because the condition does not
   * match. However, the second record has not been committed yet, so we should still retrieve it,
   * considering the possibility that the record will be rolled back.
   *
   * <p>To handle such cases, we convert the conjunctions to include conditions on the before image.
   * For example, if the original condition is:
   *
   * <pre>
   *   column = 1000
   * </pre>
   *
   * We convert it to:
   *
   * <pre>
   *   column = 1000 OR before_column = 1000
   * </pre>
   *
   * <p>Here are more examples:
   *
   * <p>Example 1:
   *
   * <pre>
   *   {@code column >= 500 AND column < 1000}
   * </pre>
   *
   * becomes:
   *
   * <pre>
   *   {@code (column >= 500 AND column < 1000) OR (before_column >= 500 AND before_column < 1000)}
   * </pre>
   *
   * <p>Example 2:
   *
   * <pre>
   *   {@code column1 = 500 OR column2 != 1000}
   * </pre>
   *
   * becomes:
   *
   * <pre>
   *   {@code column1 = 500 OR column2 != 1000 OR before_column1 = 500 OR before_column2 != 1000}
   * </pre>
   *
   * This way, we can ensure that prepared records whose before images satisfy the original scan
   * conditions are not missed during the scan.
   *
   * @param selection the selection to convert
   * @param conjunctions the conjunctions to convert
   * @return the converted conjunctions
   */
  private Set<AndConditionSet> convertConjunctions(
      Selection selection, Set<Selection.Conjunction> conjunctions) throws CrudException {
    TableMetadata metadata = getTableMetadata(selection);

    Set<AndConditionSet> converted = new HashSet<>(conjunctions.size() * 2);

    // Keep the original conjunctions
    conjunctions.forEach(
        c -> converted.add(ConditionSetBuilder.andConditionSet(c.getConditions()).build()));

    // Add conditions on the before image
    for (Selection.Conjunction conjunction : conjunctions) {
      Set<ConditionalExpression> conditions = new HashSet<>(conjunction.getConditions().size());
      for (ConditionalExpression condition : conjunction.getConditions()) {
        String columnName = condition.getColumn().getName();

        if (metadata.getPartitionKeyNames().contains(columnName)
            || metadata.getClusteringKeyNames().contains(columnName)) {
          // If the condition is on the primary key, we don't need to convert it
          conditions.add(condition);
          continue;
        }

        // Convert the condition to use the before image column
        ConditionalExpression convertedCondition;
        if (condition instanceof LikeExpression) {
          LikeExpression likeExpression = (LikeExpression) condition;
          convertedCondition =
              ConditionBuilder.buildLikeExpression(
                  likeExpression.getColumn().copyWith(Attribute.BEFORE_PREFIX + columnName),
                  likeExpression.getOperator(),
                  likeExpression.getEscape());
        } else {
          convertedCondition =
              ConditionBuilder.buildConditionalExpression(
                  condition.getColumn().copyWith(Attribute.BEFORE_PREFIX + columnName),
                  condition.getOperator());
        }

        conditions.add(convertedCondition);
      }

      converted.add(ConditionSetBuilder.andConditionSet(conditions).build());
    }

    return converted;
  }

  private Selection prepareStorageSelection(Selection selection) {
    if (selection instanceof Get) {
      return Get.newBuilder((Get) selection)
          .clearProjections()
          .consistency(Consistency.LINEARIZABLE)
          .build();
    } else {
      assert selection instanceof Scan;

      return Scan.newBuilder((Scan) selection)
          .clearProjections()
          .consistency(Consistency.LINEARIZABLE)
          .build();
    }
  }

  private TransactionTableMetadata getTransactionTableMetadata(Operation operation)
      throws CrudException {
    assert operation.forFullTableName().isPresent();

    try {
      return ConsensusCommitUtils.getTransactionTableMetadata(tableMetadataManager, operation);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(operation.forFullTableName().get()),
          e,
          snapshot.getId());
    }
  }

  private TableMetadata getTableMetadata(Operation operation) throws CrudException {
    TransactionTableMetadata metadata = getTransactionTableMetadata(operation);
    return metadata.getTableMetadata();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Snapshot getSnapshot() {
    return snapshot;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  private interface ConsensusCommitScanner extends TransactionCrudOperable.Scanner {
    boolean isClosed();
  }

  @NotThreadSafe
  private class ConsensusCommitStorageScanner extends AbstractTransactionCrudOperableScanner
      implements ConsensusCommitScanner {

    private final Scan scan;
    private final List<String> originalProjections;
    private final Scanner scanner;

    @Nullable private final LinkedHashMap<Snapshot.Key, TransactionResult> results;
    private final AtomicInteger scanCount = new AtomicInteger();
    private final AtomicBoolean fullyScanned = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ConsensusCommitStorageScanner(Scan scan, List<String> originalProjections)
        throws CrudException {
      this.scan = scan;
      this.originalProjections = originalProjections;

      if (scan.getLimit() > 0) {
        // Since recovery and conjunctions may delete some records, it is necessary to perform the
        // scan without a limit.
        scanner = scanFromStorage(Scan.newBuilder(scan).limit(0).build());
      } else {
        scanner = scanFromStorage(scan);
      }

      if (isValidationOrSnapshotReadRequired() || isOverlapVerificationRequired()) {
        results = new LinkedHashMap<>();
      } else {
        // If neither validation nor snapshot reads are required, we don't need to put the results
        // into the scan set
        results = null;
      }
    }

    @Override
    public Optional<Result> one() throws CrudException {
      if (fullyScanned.get()) {
        return Optional.empty();
      }

      try {
        while (true) {
          Optional<Result> r = scanner.one();

          if (!r.isPresent()) {
            fullyScanned.set(true);
            return Optional.empty();
          }

          TableMetadata tableMetadata = getTableMetadata(scan);
          Snapshot.Key key = new Snapshot.Key(scan, r.get(), tableMetadata);
          TransactionResult result = new TransactionResult(r.get());

          Optional<TransactionResult> processedScanResult = processScanResult(key, scan, result);
          if (!processedScanResult.isPresent()) {
            continue;
          }

          if (results != null) {
            results.put(key, processedScanResult.get());
          }
          scanCount.incrementAndGet();

          if (scan.getLimit() > 0 && scanCount.get() >= scan.getLimit()) {
            // If the scan has a limit, we stop scanning when we reach the limit.
            fullyScanned.set(true);
          }

          TableMetadata metadata = getTableMetadata(scan);
          return Optional.of(
              new FilteredResult(
                  processedScanResult.get(),
                  originalProjections,
                  metadata,
                  isIncludeMetadataEnabled));
        }
      } catch (ExecutionException e) {
        closeScanner();
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(
                e.getMessage()),
            e,
            snapshot.getId());
      } catch (CrudException e) {
        closeScanner();
        throw e;
      }
    }

    @Override
    public List<Result> all() throws CrudException {
      List<Result> results = new ArrayList<>();

      while (true) {
        Optional<Result> result = one();
        if (!result.isPresent()) {
          break;
        }
        results.add(result.get());
      }

      return results;
    }

    @Override
    public void close() {
      if (closed.get()) {
        return;
      }

      closeScanner();

      if (fullyScanned.get()) {
        // If the scanner is fully scanned, we can treat it as a normal scan, and put the results
        // into the scan set
        putIntoScanSetInSnapshot(scan, results);
      } else {
        // If the scanner is not fully scanned, put the results into the scanner set
        putIntoScannerSetInSnapshot(scan, results);
      }

      verifyNoOverlap(scan, results);
    }

    @Override
    public boolean isClosed() {
      return closed.get();
    }

    private void closeScanner() {
      closed.set(true);
      try {
        scanner.close();
      } catch (IOException e) {
        logger.warn("Failed to close the scanner. Transaction ID: {}", snapshot.getId(), e);
      }
    }
  }

  @NotThreadSafe
  private class ConsensusCommitSnapshotScanner extends AbstractTransactionCrudOperableScanner
      implements ConsensusCommitScanner {

    private final Scan scan;
    private final List<String> originalProjections;
    private final Iterator<Map.Entry<Snapshot.Key, TransactionResult>> resultsIterator;

    private final LinkedHashMap<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();
    private boolean closed;

    public ConsensusCommitSnapshotScanner(
        Scan scan,
        List<String> originalProjections,
        LinkedHashMap<Snapshot.Key, TransactionResult> resultsInSnapshot) {
      this.scan = scan;
      this.originalProjections = originalProjections;
      resultsIterator = resultsInSnapshot.entrySet().iterator();
    }

    @Override
    public Optional<Result> one() throws CrudException {
      if (!resultsIterator.hasNext()) {
        return Optional.empty();
      }

      Map.Entry<Snapshot.Key, TransactionResult> entry = resultsIterator.next();
      results.put(entry.getKey(), entry.getValue());

      TableMetadata metadata = getTableMetadata(scan);
      return Optional.of(
          new FilteredResult(
              entry.getValue(), originalProjections, metadata, isIncludeMetadataEnabled));
    }

    @Override
    public List<Result> all() throws CrudException {
      List<Result> results = new ArrayList<>();

      while (true) {
        Optional<Result> result = one();
        if (!result.isPresent()) {
          break;
        }
        results.add(result.get());
      }

      return results;
    }

    @Override
    public void close() {
      closed = true;
      verifyNoOverlap(scan, results);
    }

    @Override
    public boolean isClosed() {
      return closed;
    }
  }
}
