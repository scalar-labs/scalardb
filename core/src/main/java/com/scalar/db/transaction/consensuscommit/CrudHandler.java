package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

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
import java.util.Map.Entry;
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
    return createGetResult(key, get, originalProjections);
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
      // Keep the read set latest to create before image by using the latest record (result)
      // because another conflicting transaction might have updated the record after this
      // transaction read it first.
      snapshot.put(key, result);
      snapshot.put(get, result); // for re-read and validation
      return;
    }
    throw new UncommittedRecordException(
        get, result.get(), "This record needs recovery", snapshot.getId());
  }

  private Optional<Result> createGetResult(Snapshot.Key key, Get get, List<String> projections)
      throws CrudException {
    TableMetadata metadata = getTableMetadata(key.getNamespace(), key.getTable());
    return snapshot
        .mergeResult(key, snapshot.get(get))
        .map(r -> new FilteredResult(r, projections, metadata, isIncludeMetadataEnabled));
  }

  public List<Result> scan(Scan scan) throws CrudException {
    List<Result> results = scanInternal(scan);

    // We verify if this scan does not overlap previous writes after the actual scan. For a
    // cross-partition scan, this must be done here, using the obtained keys in the scan set and
    // scan condition. This is because the condition (i.e., where clause) is arbitrary in the
    // cross-partition scan, and thus, the write command may not have columns used in the condition,
    // which are necessary to determine overlaps. For a scan with clustering keys, we can determine
    // overlaps without the actual scan, but we also check it here for consistent logic and
    // readability.
    snapshot.verify(scan);

    return results;
  }

  private List<Result> scanInternal(Scan originalScan) throws CrudException {
    List<String> originalProjections = new ArrayList<>(originalScan.getProjections());
    Scan scan = (Scan) prepareStorageSelection(originalScan);

    Map<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>();

    Optional<Map<Snapshot.Key, TransactionResult>> resultsInSnapshot = snapshot.get(scan);
    if (resultsInSnapshot.isPresent()) {
      for (Entry<Snapshot.Key, TransactionResult> entry : resultsInSnapshot.get().entrySet()) {
        snapshot
            .mergeResult(entry.getKey(), Optional.of(entry.getValue()))
            .ifPresent(result -> results.put(entry.getKey(), result));
      }
      return createScanResults(scan, originalProjections, results);
    }

    Scanner scanner = null;
    try {
      scanner = getFromStorage(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        if (!result.isCommitted()) {
          throw new UncommittedRecordException(
              scan, result, "The record needs recovery", snapshot.getId());
        }

        Snapshot.Key key = new Snapshot.Key(scan, r);

        // We always update the read set to create before image by using the latest record (result)
        // because another conflicting transaction might have updated the record after this
        // transaction read it first.
        snapshot.put(key, Optional.of(result));

        snapshot.mergeResult(key, Optional.of(result)).ifPresent(value -> results.put(key, value));
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          logger.warn("Failed to close the scanner", e);
        }
      }
    }
    snapshot.put(scan, results);

    return createScanResults(scan, originalProjections, results);
  }

  private List<Result> createScanResults(
      Scan scan, List<String> projections, Map<Snapshot.Key, TransactionResult> results)
      throws CrudException {
    assert scan.forNamespace().isPresent() && scan.forTable().isPresent();
    TableMetadata metadata = getTableMetadata(scan.forNamespace().get(), scan.forTable().get());
    return results.values().stream()
        .map(r -> new FilteredResult(r, projections, metadata, isIncludeMetadataEnabled))
        .collect(Collectors.toList());
  }

  public void put(Put put) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(put);

    if (put.getCondition().isPresent()
        && (!put.isImplicitPreReadEnabled() && !snapshot.containsKeyInReadSet(key))) {
      throw new IllegalArgumentException(
          "Put cannot have a condition when the target record is unread and implicit pre-read is disabled."
              + " Please read the target record beforehand or enable implicit pre-read: "
              + put);
    }

    if (put.getCondition().isPresent()) {
      if (put.isImplicitPreReadEnabled() && !snapshot.containsKeyInReadSet(key)) {
        read(key, createGet(key));
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          put, snapshot.getFromReadSet(key).orElse(null));
    }

    snapshot.put(key, put);
  }

  public void delete(Delete delete) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(delete);

    if (delete.getCondition().isPresent()) {
      if (!snapshot.containsKeyInReadSet(key)) {
        read(key, createGet(key));
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          delete, snapshot.getFromReadSet(key).orElse(null));
    }

    snapshot.put(key, delete);
  }

  public void readIfImplicitPreReadEnabled() throws CrudException {
    List<ParallelExecutor.ParallelExecutorTask> tasks = new ArrayList<>();
    for (Put put : snapshot.getPutsInWriteSet()) {
      if (put.isImplicitPreReadEnabled()) {
        Snapshot.Key key = new Snapshot.Key(put);
        if (!snapshot.containsKeyInReadSet(key)) {
          tasks.add(() -> read(key, createGet(key)));
        }
      }
    }
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
      throw new CrudException("Get failed", e, snapshot.getId());
    }
  }

  private Scanner getFromStorage(Scan scan) throws CrudException {
    try {
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException("Scan failed", e, snapshot.getId());
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
            "The specified table is not found: "
                + ScalarDbUtils.getFullTableName(
                    operation.forNamespace().get(), operation.forTable().get()));
      }
      return metadata;
    } catch (ExecutionException e) {
      throw new CrudException("Getting a table metadata failed", e, snapshot.getId());
    }
  }

  private TableMetadata getTableMetadata(String namespace, String table) throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(namespace, table);
      if (metadata == null) {
        throw new IllegalArgumentException(
            "The specified table is not found: "
                + ScalarDbUtils.getFullTableName(namespace, table));
      }
      return metadata.getTableMetadata();
    } catch (ExecutionException e) {
      throw new CrudException("Getting a table metadata failed", e, snapshot.getId());
    }
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Snapshot getSnapshot() {
    return snapshot;
  }
}
