package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.util.ScalarDbUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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

  public CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      TransactionTableMetadataManager tableMetadataManager) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.tableMetadataManager = tableMetadataManager;
  }

  public Optional<Result> get(Get get) throws CrudException {
    List<String> originalProjections = new ArrayList<>(get.getProjections());

    Optional<TransactionResult> result;
    Snapshot.Key key = new Snapshot.Key(get);

    if (snapshot.containsKeyInReadSet(key)) {
      return createGetResult(key, originalProjections);
    }

    result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      snapshot.put(key, result);
      return createGetResult(key, originalProjections);
    }
    throw new UncommittedRecordException(result.get(), "this record needs recovery");
  }

  private Optional<Result> createGetResult(Snapshot.Key key, List<String> projections)
      throws CrudException {
    TableMetadata metadata = getTableMetadata(key.getNamespace(), key.getTable());
    return snapshot.get(key).map(r -> new FilteredResult(r, projections, metadata));
  }

  public List<Result> scan(Scan scan) throws CrudException {
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
          throw new UncommittedRecordException(result, "the record needs recovery");
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
          logger.warn("failed to close the scanner", e);
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
        .map(r -> new FilteredResult(r, projections, metadata))
        .collect(Collectors.toList());
  }

  public void put(Put put) {
    snapshot.put(new Snapshot.Key(put), put);
  }

  public void delete(Delete delete) {
    snapshot.put(new Snapshot.Key(delete), delete);
  }

  private Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      // get only after image columns
      get.clearProjections();
      LinkedHashSet<String> afterImageColumnNames =
          tableMetadataManager.getTransactionTableMetadata(get).getAfterImageColumnNames();
      get.withProjections(afterImageColumnNames);

      get.withConsistency(Consistency.LINEARIZABLE);
      return storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException("get failed.", e);
    }
  }

  private Scanner getFromStorage(Scan scan) throws CrudException {
    try {
      // get only after image columns
      scan.clearProjections();
      LinkedHashSet<String> afterImageColumnNames =
          tableMetadataManager.getTransactionTableMetadata(scan).getAfterImageColumnNames();
      scan.withProjections(afterImageColumnNames);

      scan.withConsistency(Consistency.LINEARIZABLE);
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException("scan failed.", e);
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
      throw new CrudException("getting a table metadata failed", e);
    }
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }
}
