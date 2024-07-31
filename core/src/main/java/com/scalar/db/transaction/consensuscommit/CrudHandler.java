package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
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
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CrudHandler {
  private static final Logger logger = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final Snapshot snapshot;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final boolean isIncludeMetadataEnabled;
  private final MutationConditionsValidator mutationConditionsValidator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = new MutationConditionsValidator(snapshot.getId());
  }

  @VisibleForTesting
  CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager,
      boolean isIncludeMetadataEnabled,
      MutationConditionsValidator mutationConditionsValidator) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
    this.mutationConditionsValidator = mutationConditionsValidator;
  }

  public Optional<Result> get(Get originalGet) throws CrudException {
    List<String> originalProjections = new ArrayList<>(originalGet.getProjections());
    Get get = (Get) prepareStorageSelection(originalGet);

    Optional<TransactionResult> result;
    Snapshot.Key key = new Snapshot.Key(get);

    if (snapshot.containsKeyInGetSet(get)) {
      return createGetResult(key, get, originalProjections);
    }

    result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      // Keep the read set latest to create before image by using the latest record (result)
      // because another conflicting transaction might have updated the record after this
      // transaction read it first.
      snapshot.put(key, result);
      snapshot.put(get, result); // for re-read and validation
      return createGetResult(key, get, originalProjections);
    }
    throw new UncommittedRecordException(
        result.get(), "this record needs recovery", snapshot.getId());
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
    // relational scan, this must be done here, using the obtained keys in the scan set and scan
    // condition. This is because the condition (i.e., where clause) is arbitrary in the relational
    // scan, and thus, the write command may not have columns used in the condition, which are
    // necessary to determine overlaps. For a scan with clustering keys, we can determine overlaps
    // without the actual scan, but we also check it here for consistent logic and readability.
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
              result, "the record needs recovery", snapshot.getId());
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
          logger.warn("failed to close the scanner", e);
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

  public void put(Put put) throws UnsatisfiedConditionException {
    mutationConditionsValidator.checkIfConditionIsSatisfied(
        put, snapshot.getFromReadSet(new Snapshot.Key(put)).orElse(null));
    snapshot.put(new Snapshot.Key(put), put);
  }

  public void delete(Delete delete) throws UnsatisfiedConditionException {
    mutationConditionsValidator.checkIfConditionIsSatisfied(
        delete, snapshot.getFromReadSet(new Snapshot.Key(delete)).orElse(null));
    snapshot.put(new Snapshot.Key(delete), delete);
  }

  @VisibleForTesting
  Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      return storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException("get failed", e, snapshot.getId());
    }
  }

  @VisibleForTesting
  Scanner getFromStorage(Scan scan) throws CrudException {
    try {
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException("scan failed", e, snapshot.getId());
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
      throw new CrudException("getting a table metadata failed", e, snapshot.getId());
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
      throw new CrudException("getting a table metadata failed", e, snapshot.getId());
    }
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Snapshot getSnapshot() {
    return snapshot;
  }
}
