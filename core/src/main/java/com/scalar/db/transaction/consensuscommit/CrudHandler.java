package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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

  public Optional<Result> get(Get get) throws CrudException {
    List<String> originalProjections = new ArrayList<>(get.getProjections());
    Snapshot.Key key = new Snapshot.Key(get);
    readUnread(key, get);
    return createGetResult(key, originalProjections);
  }

  @VisibleForTesting
  void readUnread(Snapshot.Key key, Get get) throws CrudException {
    if (!snapshot.containsKeyInReadSet(key)) {
      read(key, get);
    }
  }

  private void read(Snapshot.Key key, Get get) throws CrudException {
    Optional<TransactionResult> result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      snapshot.put(key, result); // for read set
      snapshot.put(get, key); // for extra-read using Get
      return;
    }
    throw new UncommittedRecordException(
        get,
        result.get(),
        CoreError.CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD.buildMessage(),
        snapshot.getId());
  }

  private Optional<Result> createGetResult(Snapshot.Key key, List<String> projections)
      throws CrudException {
    TableMetadata metadata = getTableMetadata(key.getNamespace(), key.getTable());
    return snapshot
        .get(key)
        .map(r -> new FilteredResult(r, projections, metadata, isIncludeMetadataEnabled));
  }

  public List<Result> scan(Scan scan) throws CrudException {
    List<Result> results = scanInternal(scan);

    // We verify if this scan does not overlap previous writes using the actual scan result. Because
    // we support arbitrary conditions in the where clause of a scan (not only ScanAll, but also
    // Scan and ScanWithIndex), we cannot determine whether the scan results will include a record
    // whose key is the same as the key specified in the previous writes, without knowing the
    // obtained keys in the actual scan. With this check, users can avoid seeing unexpected scan
    // results that have not included previous writes yet.
    snapshot.verify(scan);

    return results;
  }

  private List<Result> scanInternal(Scan scan) throws CrudException {
    List<String> originalProjections = new ArrayList<>(scan.getProjections());

    List<Result> results = new ArrayList<>();

    Optional<List<Snapshot.Key>> keysInSnapshot = snapshot.get(scan);
    if (keysInSnapshot.isPresent()) {
      for (Snapshot.Key key : keysInSnapshot.get()) {
        snapshot.get(key).ifPresent(results::add);
      }
      return createScanResults(scan, originalProjections, results);
    }

    List<Snapshot.Key> keys = new ArrayList<>();
    Scanner scanner = null;
    try {
      scanner = scanFromStorage(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        if (!result.isCommitted()) {
          throw new UncommittedRecordException(
              scan,
              result,
              CoreError.CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD.buildMessage(),
              snapshot.getId());
        }

        Snapshot.Key key = new Snapshot.Key(scan, r);

        if (!snapshot.containsKeyInReadSet(key)) {
          snapshot.put(key, Optional.of(result));
        }

        keys.add(key);
        snapshot.get(key).ifPresent(results::add);
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
    snapshot.put(scan, keys);

    return createScanResults(scan, originalProjections, results);
  }

  private List<Result> createScanResults(Scan scan, List<String> projections, List<Result> results)
      throws CrudException {
    TableMetadata metadata = getTableMetadata(scan.forNamespace().get(), scan.forTable().get());
    return results.stream()
        .map(r -> new FilteredResult(r, projections, metadata, isIncludeMetadataEnabled))
        .collect(Collectors.toList());
  }

  public void put(Put put) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(put);

    if (put.getCondition().isPresent()
        && (!put.isImplicitPreReadEnabled() && !snapshot.containsKeyInReadSet(key))) {
      throw new IllegalArgumentException(
          CoreError
              .CONSENSUS_COMMIT_PUT_CANNOT_HAVE_CONDITION_WHEN_TARGET_RECORD_UNREAD_AND_IMPLICIT_PRE_READ_DISABLED
              .buildMessage(put));
    }

    if (put.getCondition().isPresent()) {
      if (put.isImplicitPreReadEnabled()) {
        readUnread(key, createGet(key));
      }
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          put, snapshot.getFromReadSet(key).orElse(null));
    }

    snapshot.put(key, put);
  }

  public void delete(Delete delete) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(delete);

    if (delete.getCondition().isPresent()) {
      readUnread(key, createGet(key));
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          delete, snapshot.getFromReadSet(key).orElse(null));
    }

    snapshot.put(key, delete);
  }

  public void readIfImplicitPreReadEnabled() throws CrudException {
    List<ParallelExecutor.ParallelExecutorTask> tasks = new ArrayList<>();

    // For each put in the write set, if implicit pre-read is enabled and the record is not read
    // yet, read the record
    for (Put put : snapshot.getPutsInWriteSet()) {
      if (put.isImplicitPreReadEnabled()) {
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
    return buildableGet.build();
  }

  // Although this class is not thread-safe, this method is actually thread-safe because the storage
  // is thread-safe
  @VisibleForTesting
  Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      get.clearProjections();
      // Retrieve only the after images columns when including the metadata is disabled, otherwise
      // retrieve all the columns
      if (!isIncludeMetadataEnabled) {
        LinkedHashSet<String> afterImageColumnNames =
            tableMetadataManager.getTransactionTableMetadata(get).getAfterImageColumnNames();
        get.withProjections(afterImageColumnNames);
      }
      get.withConsistency(Consistency.LINEARIZABLE);
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
      scan.clearProjections();
      // Retrieve only the after images columns when including the metadata is disabled, otherwise
      // retrieve all the columns
      if (!isIncludeMetadataEnabled) {
        LinkedHashSet<String> afterImageColumnNames =
            tableMetadataManager.getTransactionTableMetadata(scan).getAfterImageColumnNames();
        scan.withProjections(afterImageColumnNames);
      }
      scan.withConsistency(Consistency.LINEARIZABLE);
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException(
          CoreError.CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED.buildMessage(),
          e,
          snapshot.getId());
    }
  }

  private TableMetadata getTableMetadata(String namespace, String table) throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(namespace, table);
      if (metadata == null) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(
                ScalarDbUtils.getFullTableName(namespace, table)));
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
