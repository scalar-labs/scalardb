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

    if (snapshot.containsKeyInReadSet(key)) {
      return createGetResult(key, originalProjections);
    }

    Optional<TransactionResult> result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      snapshot.put(key, result);
      return createGetResult(key, originalProjections);
    }
    throw new UncommittedRecordException(
        result.get(), "This record needs recovery", snapshot.getId());
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

    // We verify if this scan does not overlap previous writes after the actual scan. For a
    // relational scan, this must be done here, using the obtained keys in the scan set and scan
    // condition. This is because the condition (i.e., where clause) is arbitrary in the relational
    // scan, and thus, the write command may not have columns used in the condition, which are
    // necessary to determine overlaps. For a scan with clustering keys, we can determine overlaps
    // without the actual scan, but we also check it here for consistent logic and readability.
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
      scanner = getFromStorage(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        if (!result.isCommitted()) {
          throw new UncommittedRecordException(
              result, "The record needs recovery", snapshot.getId());
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
    if (put.getCondition().isPresent() && put.isBlind()) {
      throw new IllegalArgumentException("Blind put cannot have a condition: " + put);
    }

    Snapshot.Key key = new Snapshot.Key(put);

    if (put.getCondition().isPresent()) {
      fillReadSetIfUnread(key);
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          put, snapshot.getFromReadSet(key).orElse(null));
    }

    snapshot.put(key, put);
  }

  public void delete(Delete delete) throws CrudException {
    Snapshot.Key key = new Snapshot.Key(delete);

    if (delete.getCondition().isPresent()) {
      fillReadSetIfUnread(key);
      mutationConditionsValidator.checkIfConditionIsSatisfied(
          delete, snapshot.getFromReadSet(key).orElse(null));
    }

    snapshot.put(key, delete);
  }

  @VisibleForTesting
  void fillReadSetIfUnread(Snapshot.Key key) throws CrudException {
    if (!snapshot.containsKeyInReadSet(key)) {
      fillReadSet(key);
    }
  }

  private void fillReadSet(Snapshot.Key key) throws CrudException {
    Optional<TransactionResult> result = getFromStorage(createGet(key));
    if (!result.isPresent() || result.get().isCommitted()) {
      snapshot.put(key, result);
      return;
    }
    throw new UncommittedRecordException(
        result.get(), "This record needs recovery", snapshot.getId());
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

  public void fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread() throws CrudException {
    List<ParallelExecutor.ParallelExecutorTask> tasks = new ArrayList<>();
    for (Put put : snapshot.getPutsInWriteSet()) {
      if (!put.isBlind()) {
        Snapshot.Key key = new Snapshot.Key(put);
        if (!snapshot.containsKeyInReadSet(key)) {
          tasks.add(() -> fillReadSet(key));
        }
      }
    }
    for (Delete delete : snapshot.getDeletesInDeleteSet()) {
      Snapshot.Key key = new Snapshot.Key(delete);
      if (!snapshot.containsKeyInReadSet(key)) {
        tasks.add(() -> fillReadSet(key));
      }
    }

    parallelExecutor.fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread(tasks, snapshot.getId());
  }

  private Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
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
      throw new CrudException("Get failed", e, snapshot.getId());
    }
  }

  private Scanner getFromStorage(Scan scan) throws CrudException {
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
      throw new CrudException("Scan failed", e, snapshot.getId());
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
