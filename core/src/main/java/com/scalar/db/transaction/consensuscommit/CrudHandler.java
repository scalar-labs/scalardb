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
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final TransactionTableMetadataManager tableMetadataManager;
  private final boolean isIncludeMetadataEnabled;
  private final MutationConditionsValidator mutationConditionsValidator;
  private final ParallelExecutor parallelExecutor;

  // Whether the transaction is in read-only mode or not.
  private final boolean readOnly;

  private final List<ConsensusCommitScanner> scanners = new ArrayList<>();

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      ParallelExecutor parallelExecutor,
      boolean readOnly) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = new MutationConditionsValidator(snapshot.getId());
    this.parallelExecutor = parallelExecutor;
    this.readOnly = readOnly;
  }

  @VisibleForTesting
  CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      MutationConditionsValidator mutationConditionsValidator,
      ParallelExecutor parallelExecutor,
      boolean readOnly) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = mutationConditionsValidator;
    this.parallelExecutor = parallelExecutor;
    this.readOnly = readOnly;
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

    readUnread(key, get);

    return snapshot
        .getResult(key, get)
        .map(r -> new FilteredResult(r, originalProjections, metadata, isIncludeMetadataEnabled));
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
  void read(@Nullable Snapshot.Key key, Get get) throws CrudException {
    Optional<TransactionResult> result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      if (result.isPresent() || get.getConjunctions().isEmpty()) {
        // Keep the read set latest to create before image by using the latest record (result)
        // because another conflicting transaction might have updated the record after this
        // transaction read it first. However, we update it only if a get operation has no
        // conjunction or the result exists. This is because we don’t know whether the record
        // actually exists or not due to the conjunction.
        if (key != null) {
          putIntoReadSetInSnapshot(key, result);
        } else {
          // Only for a Get with index, the argument `key` is null

          if (result.isPresent()) {
            // Only when we can get the record with the Get with index, we can put it into the read
            // set
            key = new Snapshot.Key(get, result.get());
            putIntoReadSetInSnapshot(key, result);
          }
        }
      }
      snapshot.putIntoGetSet(get, result);
      return;
    }
    throw new UncommittedRecordException(
        get,
        result.get(),
        CoreError.CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD.buildMessage(),
        snapshot.getId());
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
      scanner = scanFromStorage(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        Snapshot.Key key = new Snapshot.Key(scan, r);
        processScanResult(key, scan, result);
        results.put(key, result);
      }
    } catch (RuntimeException e) {
      Exception exception;
      if (e.getCause() instanceof ExecutionException) {
        exception = (ExecutionException) e.getCause();
      } else {
        exception = e;
      }
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(),
          exception,
          snapshot.getId());
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          logger.warn("Failed to close the scanner", e);
        }
      }
    }

    snapshot.putIntoScanSet(scan, results);

    return results;
  }

  private void processScanResult(Snapshot.Key key, Scan scan, TransactionResult result)
      throws CrudException {
    if (!result.isCommitted()) {
      throw new UncommittedRecordException(
          scan,
          result,
          CoreError.CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD.buildMessage(),
          snapshot.getId());
    }

    // We always update the read set to create before image by using the latest record (result)
    // because another conflicting transaction might have updated the record after this
    // transaction read it first.
    putIntoReadSetInSnapshot(key, Optional.of(result));
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
    if (!readOnly) {
      snapshot.putIntoReadSet(key, result);
    }
  }

  private void verifyNoOverlap(Scan scan, Map<Snapshot.Key, TransactionResult> results) {
    // In read-only mode, we don't need to verify the overlap
    if (!readOnly) {
      snapshot.verifyNoOverlap(scan, results);
    }
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

  private Get createGet(Snapshot.Key key) throws CrudException {
    GetBuilder.BuildableGet buildableGet =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey());
    key.getClusteringKey().ifPresent(buildableGet::clusteringKey);
    return (Get) prepareStorageSelection(buildableGet.build());
  }

  // Although this class is not thread-safe, this method is actually thread-safe because the storage
  // is thread-safe
  @VisibleForTesting
  Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      return storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_READING_RECORD_FROM_STORAGE_FAILED.buildMessage(),
          e,
          snapshot.getId());
    }
  }

  private Scanner scanFromStorage(Scan scan) throws CrudException {
    try {
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(),
          e,
          snapshot.getId());
    }
  }

  private Selection prepareStorageSelection(Selection selection) throws CrudException {
    selection.clearProjections();
    // Retrieve only the after images columns when including the metadata is disabled, otherwise
    // retrieve all the columns
    if (!isIncludeMetadataEnabled) {
      LinkedHashSet<String> afterImageColumnNames =
          getTransactionTableMetadata(selection).getAfterImageColumnNames();
      selection.withProjections(afterImageColumnNames);
    }
    selection.withConsistency(Consistency.LINEARIZABLE);
    return selection;
  }

  private TransactionTableMetadata getTransactionTableMetadata(Operation operation)
      throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(operation);
      if (metadata == null) {
        assert operation.forNamespace().isPresent() && operation.forTable().isPresent();
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(
                ScalarDbUtils.getFullTableName(
                    operation.forNamespace().get(), operation.forTable().get())));
      }
      return metadata;
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(), e, snapshot.getId());
    }
  }

  private TableMetadata getTableMetadata(Operation operation) throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(operation);
      if (metadata == null) {
        assert operation.forFullTableName().isPresent();
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(operation.forFullTableName().get()));
      }
      return metadata.getTableMetadata();
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, snapshot.getId());
    }
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Snapshot getSnapshot() {
    return snapshot;
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

    private final LinkedHashMap<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();
    private final AtomicBoolean fullyScanned = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ConsensusCommitStorageScanner(Scan scan, List<String> originalProjections)
        throws CrudException {
      this.scan = scan;
      this.originalProjections = originalProjections;
      scanner = scanFromStorage(scan);
    }

    @Override
    public Optional<Result> one() throws CrudException {
      try {
        Optional<Result> r = scanner.one();

        if (!r.isPresent()) {
          fullyScanned.set(true);
          return Optional.empty();
        }

        Snapshot.Key key = new Snapshot.Key(scan, r.get());
        TransactionResult result = new TransactionResult(r.get());
        processScanResult(key, scan, result);
        results.put(key, result);

        TableMetadata metadata = getTableMetadata(scan);
        return Optional.of(
            new FilteredResult(result, originalProjections, metadata, isIncludeMetadataEnabled));
      } catch (ExecutionException e) {
        closeScanner();
        throw new CrudException(
            CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(),
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
        snapshot.putIntoScanSet(scan, results);
      } else {
        // If the scanner is not fully scanned, put the results into the scanner set
        snapshot.putIntoScannerSet(scan, results);
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
        logger.warn("Failed to close the scanner", e);
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
