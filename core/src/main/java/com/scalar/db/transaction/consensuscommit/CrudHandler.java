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
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = new MutationConditionsValidator(snapshot.getId());
    this.parallelExecutor = parallelExecutor;
  }

  @VisibleForTesting
  CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      MutationConditionsValidator mutationConditionsValidator,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = mutationConditionsValidator;
    this.parallelExecutor = parallelExecutor;
  }

  public Optional<Result> get(Get originalGet) throws CrudException {
    List<String> originalProjections = new ArrayList<>(originalGet.getProjections());
    Get get = (Get) prepareStorageSelection(originalGet);
    Snapshot.Key key = new Snapshot.Key(get);
    readUnread(key, get);

    TableMetadata metadata = getTableMetadata(get);
    return snapshot
        .getResult(key, get)
        .map(r -> new FilteredResult(r, originalProjections, metadata, isIncludeMetadataEnabled));
  }

  @VisibleForTesting
  void readUnread(Snapshot.Key key, Get get) throws CrudException {
    if (!snapshot.containsKeyInGetSet(get)) {
      read(key, get);
    }
  }

  // Although this class is not thread-safe, this method is actually thread-safe, so we call it
  // concurrently in the implicit pre-read
  @VisibleForTesting
  void read(Snapshot.Key key, Get get) throws CrudException {
    Optional<TransactionResult> result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      if (result.isPresent() || get.getConjunctions().isEmpty()) {
        // Keep the read set latest to create before image by using the latest record (result)
        // because another conflicting transaction might have updated the record after this
        // transaction read it first. However, we update it only if a get operation has no
        // conjunction or the result exists. This is because we don’t know whether the record
        // actually exists or not due to the conjunction.
        snapshot.putIntoReadSet(key, result);
      }
      snapshot.putIntoGetSet(get, result); // for re-read and validation
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
    Map<Snapshot.Key, TransactionResult> results = scanInternal(scan);
    snapshot.verifyNoOverlap(scan, results);

    TableMetadata metadata = getTableMetadata(scan);
    return results.values().stream()
        .map(r -> new FilteredResult(r, originalProjections, metadata, isIncludeMetadataEnabled))
        .collect(Collectors.toList());
  }

  private Map<Snapshot.Key, TransactionResult> scanInternal(Scan scan) throws CrudException {
    Optional<Map<Snapshot.Key, TransactionResult>> resultsInSnapshot = snapshot.getResults(scan);
    if (resultsInSnapshot.isPresent()) {
      return resultsInSnapshot.get();
    }

    Map<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();

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
    snapshot.putIntoReadSet(key, Optional.of(result));
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
}
