package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes.isImplicitPreReadEnabled;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CrudHandler {
  private static final Logger logger = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final RecoveryExecutor recoveryExecutor;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final boolean isIncludeMetadataEnabled;
  private final MutationConditionsValidator mutationConditionsValidator;
  private final ParallelExecutor parallelExecutor;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CrudHandler(
      DistributedStorage storage,
      RecoveryExecutor recoveryExecutor,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = new MutationConditionsValidator();
    this.parallelExecutor = checkNotNull(parallelExecutor);
  }

  @VisibleForTesting
  CrudHandler(
      DistributedStorage storage,
      RecoveryExecutor recoveryExecutor,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      MutationConditionsValidator mutationConditionsValidator,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = checkNotNull(mutationConditionsValidator);
    this.parallelExecutor = checkNotNull(parallelExecutor);
  }

  public Optional<Result> get(Get get, TransactionContext context) throws CrudException {
    TableMetadata metadata = getTableMetadata(get, context.transactionId);

    Snapshot.Key key;
    if (ScalarDbUtils.isSecondaryIndexSpecified(get, metadata)) {
      // In case of a Get with index, we don't know the key until we read the record
      key = null;
    } else {
      key = new Snapshot.Key(get);
    }

    if (isSnapshotReadRequired(context)) {
      readUnread(key, get, context, metadata);
      return context
          .snapshot
          .getResult(key, get)
          .map(
              r -> new FilteredResult(r, get.getProjections(), metadata, isIncludeMetadataEnabled));
    } else {
      Optional<TransactionResult> result = read(key, get, context, metadata);
      return context
          .snapshot
          .mergeResult(key, result, get.getConjunctions())
          .map(
              r -> new FilteredResult(r, get.getProjections(), metadata, isIncludeMetadataEnabled));
    }
  }

  // Only for a Get with index, the argument `key` is null
  @VisibleForTesting
  void readUnread(
      @Nullable Snapshot.Key key, Get get, TransactionContext context, TableMetadata metadata)
      throws CrudException {
    if (!context.snapshot.containsKeyInGetSet(get)) {
      read(key, get, context, metadata);
    }
  }

  // Although this class is not thread-safe, this method is actually thread-safe, so we call it
  // concurrently in the implicit pre-read
  @VisibleForTesting
  Optional<TransactionResult> read(
      @Nullable Snapshot.Key key, Get get, TransactionContext context, TableMetadata metadata)
      throws CrudException {
    Optional<TransactionResult> result = getFromStorage(get, metadata, context.transactionId);
    if (result.isPresent() && !result.get().isCommitted()) {
      // Lazy recovery

      if (key == null) {
        // Only for a Get with index, the argument `key` is null. In that case, create a key from
        // the result
        key = new Snapshot.Key(get, result.get(), metadata);
      }

      result = executeRecovery(key, get, result.get(), context);
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
        putIntoReadSetInSnapshot(key, result, context);
      } else {
        // Only for a Get with index, the argument `key` is null

        if (result.isPresent()) {
          // Only when we can get the record with the Get with index, we can put it into the read
          // set
          key = new Snapshot.Key(get, result.get(), metadata);
          putIntoReadSetInSnapshot(key, result, context);
        }
      }
    }
    putIntoGetSetInSnapshot(get, result, context);
    return result;
  }

  private Optional<TransactionResult> executeRecovery(
      Snapshot.Key key, Selection selection, TransactionResult result, TransactionContext context)
      throws CrudException {
    RecoveryExecutor.RecoveryType recoveryType;
    if (context.isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      if (context.readOnly) {
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
        recoveryExecutor.execute(key, selection, result, context.transactionId, recoveryType);

    context.recoveryResults.add(recoveryResult);
    return recoveryResult.recoveredResult;
  }

  public List<Result> scan(Scan scan, TransactionContext context) throws CrudException {
    TableMetadata metadata = getTableMetadata(scan, context.transactionId);
    LinkedHashMap<Snapshot.Key, TransactionResult> results = scanInternal(scan, context, metadata);
    verifyNoOverlap(scan, results, context);
    return results.values().stream()
        .map(r -> new FilteredResult(r, scan.getProjections(), metadata, isIncludeMetadataEnabled))
        .collect(Collectors.toList());
  }

  private LinkedHashMap<Snapshot.Key, TransactionResult> scanInternal(
      Scan scan, TransactionContext context, TableMetadata metadata) throws CrudException {
    Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> resultsInSnapshot =
        context.snapshot.getResults(scan);
    if (resultsInSnapshot.isPresent()) {
      return resultsInSnapshot.get();
    }

    LinkedHashMap<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();

    try (Scanner scanner = scanFromStorage(scan, metadata, context.transactionId)) {
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        Snapshot.Key key = new Snapshot.Key(scan, r, metadata);
        Optional<TransactionResult> processedScanResult =
            processScanResult(key, scan, result, context);
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
          context.transactionId);
    } catch (IOException e) {
      logger.warn("Failed to close the scanner. Transaction ID: {}", context.transactionId, e);
    }

    putIntoScanSetInSnapshot(scan, results, context);

    return results;
  }

  private Optional<TransactionResult> processScanResult(
      Snapshot.Key key, Scan scan, TransactionResult result, TransactionContext context)
      throws CrudException {
    Optional<TransactionResult> ret;
    if (!result.isCommitted()) {
      // Lazy recovery
      ret = executeRecovery(key, scan, result, context);
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
      putIntoReadSetInSnapshot(key, ret, context);
    }

    return ret;
  }

  public TransactionCrudOperable.Scanner getScanner(Scan scan, TransactionContext context)
      throws CrudException {
    TableMetadata metadata = getTableMetadata(scan, context.transactionId);
    ConsensusCommitScanner scanner;
    Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> resultsInSnapshot =
        context.snapshot.getResults(scan);
    if (resultsInSnapshot.isPresent()) {
      scanner =
          new ConsensusCommitSnapshotScanner(scan, context, metadata, resultsInSnapshot.get());
    } else {
      scanner = new ConsensusCommitStorageScanner(scan, context, metadata);
    }

    context.scanners.add(scanner);
    return scanner;
  }

  private void putIntoReadSetInSnapshot(
      Snapshot.Key key, Optional<TransactionResult> result, TransactionContext context) {
    // In read-only mode, we don't need to put the result into the read set
    if (!context.readOnly && !context.snapshot.containsKeyInReadSet(key)) {
      context.snapshot.putIntoReadSet(key, result);
    }
  }

  private boolean isSnapshotReadRequired(TransactionContext context) {
    // In one-operation mode, we don't need snapshot reads
    return !context.oneOperation && context.isSnapshotReadRequired();
  }

  private boolean isValidationOrSnapshotReadRequired(TransactionContext context) {
    return context.isValidationPossiblyRequired() || isSnapshotReadRequired(context);
  }

  private void putIntoGetSetInSnapshot(
      Get get, Optional<TransactionResult> result, TransactionContext context) {
    // If neither validation nor snapshot reads are required, we don't need to put the result into
    // the get set
    if (isValidationOrSnapshotReadRequired(context)) {
      context.snapshot.putIntoGetSet(get, result);
    }
  }

  private void putIntoScanSetInSnapshot(
      Scan scan,
      LinkedHashMap<Snapshot.Key, TransactionResult> results,
      TransactionContext context) {
    // If neither validation nor snapshot reads are required, we don't need to put the results into
    // the scan set
    if (isValidationOrSnapshotReadRequired(context)) {
      context.snapshot.putIntoScanSet(scan, results);
    }
  }

  private void putIntoScannerSetInSnapshot(
      Scan scan,
      LinkedHashMap<Snapshot.Key, TransactionResult> results,
      TransactionContext context) {
    // if validation is not required, we don't need to put the results into the scanner set
    if (context.isValidationPossiblyRequired()) {
      context.snapshot.putIntoScannerSet(scan, results);
    }
  }

  private void verifyNoOverlap(
      Scan scan, Map<Snapshot.Key, TransactionResult> results, TransactionContext context) {
    if (isOverlapVerificationRequired(context)) {
      context.snapshot.verifyNoOverlap(scan, results);
    }
  }

  private boolean isOverlapVerificationRequired(TransactionContext context) {
    // In either read-only mode or one-operation mode, we don't need to verify overlap
    return !context.readOnly && !context.oneOperation;
  }

  public void put(Put put, TransactionContext context) throws CrudException {
    TableMetadata metadata = getTableMetadata(put, context.transactionId);
    Snapshot.Key key = new Snapshot.Key(put);

    if (put.getCondition().isPresent()
        && (!isImplicitPreReadEnabled(put) && !context.snapshot.containsKeyInReadSet(key))) {
      throw new IllegalArgumentException(
          CoreError
              .CONSENSUS_COMMIT_PUT_CANNOT_HAVE_CONDITION_WHEN_TARGET_RECORD_UNREAD_AND_IMPLICIT_PRE_READ_DISABLED
              .buildMessage(put));
    }

    if (put.getCondition().isPresent()) {
      if (isImplicitPreReadEnabled(put) && !context.snapshot.containsKeyInReadSet(key)) {
        read(key, createGet(key), context, metadata);
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          put, context.snapshot.getResult(key).orElse(null), context.transactionId);
    }

    context.snapshot.putIntoWriteSet(key, put);
  }

  public void delete(Delete delete, TransactionContext context) throws CrudException {
    TableMetadata metadata = getTableMetadata(delete, context.transactionId);
    Snapshot.Key key = new Snapshot.Key(delete);

    if (delete.getCondition().isPresent()) {
      if (!context.snapshot.containsKeyInReadSet(key)) {
        read(key, createGet(key), context, metadata);
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          delete, context.snapshot.getResult(key).orElse(null), context.transactionId);
    }

    context.snapshot.putIntoDeleteSet(key, delete);
  }

  public void readIfImplicitPreReadEnabled(TransactionContext context) throws CrudException {
    List<ParallelExecutor.ParallelExecutorTask> tasks = new ArrayList<>();

    // For each put in the write set, if implicit pre-read is enabled and the record is not read
    // yet, read the record
    for (Map.Entry<Snapshot.Key, Put> entry : context.snapshot.getWriteSet()) {
      Put put = entry.getValue();
      if (isImplicitPreReadEnabled(put)) {
        Snapshot.Key key = entry.getKey();
        if (!context.snapshot.containsKeyInReadSet(key)) {
          Get get = createGet(key);
          TableMetadata metadata = getTableMetadata(get, context.transactionId);
          tasks.add(() -> read(key, get, context, metadata));
        }
      }
    }

    // For each delete in the write set, if the record is not read yet, read the record
    for (Map.Entry<Snapshot.Key, Delete> entry : context.snapshot.getDeleteSet()) {
      Snapshot.Key key = entry.getKey();
      if (!context.snapshot.containsKeyInReadSet(key)) {
        Get get = createGet(key);
        TableMetadata metadata = getTableMetadata(get, context.transactionId);
        tasks.add(() -> read(key, get, context, metadata));
      }
    }

    if (!tasks.isEmpty()) {
      parallelExecutor.executeImplicitPreRead(tasks, context.transactionId);
    }
  }

  private Get createGet(Snapshot.Key key) {
    GetBuilder.BuildableGet buildableGet =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey());
    key.getClusteringKey().ifPresent(buildableGet::clusteringKey);
    return buildableGet.consistency(Consistency.LINEARIZABLE).build();
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
   * @param context the transaction context
   * @throws CrudConflictException if any recovery task fails due to a conflict
   * @throws CrudException if any recovery task fails
   */
  public void waitForRecoveryCompletionIfNecessary(TransactionContext context)
      throws CrudException {
    for (RecoveryExecutor.Result recoveryResult : context.recoveryResults) {
      try {
        if (context.snapshot.containsKeyInWriteSet(recoveryResult.key)
            || context.snapshot.containsKeyInDeleteSet(recoveryResult.key)
            || context.isValidationPossiblyRequired()) {
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
            context.transactionId);
      } catch (Exception e) {
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
            e,
            context.transactionId);
      }
    }
  }

  @VisibleForTesting
  void waitForRecoveryCompletion(TransactionContext context) throws CrudException {
    for (RecoveryExecutor.Result recoveryResult : context.recoveryResults) {
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
            context.transactionId);
      } catch (Exception e) {
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_RECOVERING_RECORDS_FAILED.buildMessage(e.getMessage()),
            e,
            context.transactionId);
      }
    }
  }

  // Although this class is not thread-safe, this method is actually thread-safe because the storage
  // is thread-safe
  @VisibleForTesting
  Optional<TransactionResult> getFromStorage(Get get, TableMetadata metadata, String transactionId)
      throws CrudException {
    try {
      return storage
          .get(ConsensusCommitUtils.prepareGetForStorage(get, metadata))
          .map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_READING_RECORD_FROM_STORAGE_FAILED.buildMessage(
              e.getMessage()),
          e,
          transactionId);
    }
  }

  private Scanner scanFromStorage(Scan scan, TableMetadata metadata, String transactionId)
      throws CrudException {
    try {
      return storage.scan(ConsensusCommitUtils.prepareScanForStorage(scan, metadata));
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(
              e.getMessage()),
          e,
          transactionId);
    }
  }

  private TableMetadata getTableMetadata(Operation operation, String transactionId)
      throws CrudException {
    assert operation.forFullTableName().isPresent();

    try {
      return ConsensusCommitUtils.getTransactionTableMetadata(tableMetadataManager, operation)
          .getTableMetadata();
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(operation.forFullTableName().get()),
          e,
          transactionId);
    }
  }

  @NotThreadSafe
  private class ConsensusCommitStorageScanner extends AbstractTransactionCrudOperableScanner
      implements ConsensusCommitScanner {

    private final Scan scan;
    private final TransactionContext context;
    private final TableMetadata metadata;
    private final Scanner scanner;

    @Nullable private final LinkedHashMap<Snapshot.Key, TransactionResult> results;
    private final AtomicInteger scanCount = new AtomicInteger();
    private final AtomicBoolean fullyScanned = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ConsensusCommitStorageScanner(
        Scan scan, TransactionContext context, TableMetadata metadata) throws CrudException {
      this.scan = scan;
      this.context = context;
      this.metadata = metadata;

      scanner = scanFromStorage(scan, metadata, context.transactionId);

      if (isValidationOrSnapshotReadRequired(context) || isOverlapVerificationRequired(context)) {
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

          Snapshot.Key key = new Snapshot.Key(scan, r.get(), metadata);
          TransactionResult result = new TransactionResult(r.get());

          Optional<TransactionResult> processedScanResult =
              processScanResult(key, scan, result, context);
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

          return Optional.of(
              new FilteredResult(
                  processedScanResult.get(),
                  scan.getProjections(),
                  metadata,
                  isIncludeMetadataEnabled));
        }
      } catch (ExecutionException e) {
        closeScanner();
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(
                e.getMessage()),
            e,
            context.transactionId);
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
        putIntoScanSetInSnapshot(scan, results, context);
      } else {
        // If the scanner is not fully scanned, put the results into the scanner set
        putIntoScannerSetInSnapshot(scan, results, context);
      }

      verifyNoOverlap(scan, results, context);
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
        logger.warn("Failed to close the scanner. Transaction ID: {}", context.transactionId, e);
      }
    }
  }

  @NotThreadSafe
  private class ConsensusCommitSnapshotScanner extends AbstractTransactionCrudOperableScanner
      implements ConsensusCommitScanner {

    private final Scan scan;
    private final TransactionContext context;
    private final TableMetadata metadata;
    private final Iterator<Map.Entry<Snapshot.Key, TransactionResult>> resultsIterator;

    private final LinkedHashMap<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();
    private boolean closed;

    public ConsensusCommitSnapshotScanner(
        Scan scan,
        TransactionContext context,
        TableMetadata metadata,
        LinkedHashMap<Snapshot.Key, TransactionResult> resultsInSnapshot) {
      this.scan = scan;
      this.context = context;
      this.metadata = metadata;
      resultsIterator = resultsInSnapshot.entrySet().iterator();
    }

    @Override
    public Optional<Result> one() {
      if (!resultsIterator.hasNext()) {
        return Optional.empty();
      }

      Map.Entry<Snapshot.Key, TransactionResult> entry = resultsIterator.next();
      results.put(entry.getKey(), entry.getValue());

      return Optional.of(
          new FilteredResult(
              entry.getValue(), scan.getProjections(), metadata, isIncludeMetadataEnabled));
    }

    @Override
    public List<Result> all() {
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
      verifyNoOverlap(scan, results, context);
    }

    @Override
    public boolean isClosed() {
      return closed;
    }
  }
}
