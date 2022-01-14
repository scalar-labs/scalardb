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
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CrudHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final Snapshot snapshot;

  public CrudHandler(DistributedStorage storage, Snapshot snapshot) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
  }

  public Optional<Result> get(Get get) throws CrudException {
    List<String> projections = new ArrayList<>(get.getProjections());
    Snapshot.Key key = new Snapshot.Key(get);
    if (!snapshot.containsKeyInReadSet(key)) {
      getFromStorageAndPutIntoSnapshot(get, key);
    }
    return snapshot.get(get).map(r -> new FilteredResult(r, projections));
  }

  public List<Result> scan(Scan scan) throws CrudException {
    List<String> projections = new ArrayList<>(scan.getProjections());
    if (!snapshot.containsKeyInScanSet(scan)) {
      getFromStorageAndPutIntoSnapshot(scan);
    }
    return snapshot.scan(scan).stream()
        .map(r -> new FilteredResult(r, projections))
        .collect(Collectors.toList());
  }

  public void put(Put put) throws CrudException {
    snapshot.put(new Snapshot.Key(put), put);
  }

  public void delete(Delete delete) throws CrudException {
    snapshot.put(new Snapshot.Key(delete), delete);
  }

  private void getFromStorageAndPutIntoSnapshot(Get get, Snapshot.Key key) throws CrudException {
    Optional<TransactionResult> result;
    try {
      get.clearProjections(); // project all
      get.withConsistency(Consistency.LINEARIZABLE);
      result = storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException("get failed.", e);
    }

    if (!result.isPresent()) {
      snapshot.put(key, get, Optional.empty());
      return;
    }
    if (result.get().isCommitted()) {
      snapshot.put(key, get, result);
      return;
    }

    throw new UncommittedRecordException(result.get(), "this record needs recovery");
  }

  private void getFromStorageAndPutIntoSnapshot(Scan scan) throws CrudException {
    List<Snapshot.Key> keys = new ArrayList<>();
    Scanner scanner = null;
    try {
      scan.clearProjections(); // project all
      scan.withConsistency(Consistency.LINEARIZABLE);
      scanner = storage.scan(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        if (!result.isCommitted()) {
          throw new UncommittedRecordException(result, "the record needs recovery");
        }

        Snapshot.Key key = getSnapshotKey(r, scan);

        if (!snapshot.containsKeyInReadSet(key)) {
          snapshot.put(key, scan, Optional.of(result));
        }

        keys.add(key);
      }
    } catch (ExecutionException e) {
      throw new CrudException("scan failed.", e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          LOGGER.warn("failed to close the scanner", e);
        }
      }
    }
    snapshot.put(scan, keys);
  }

  private Snapshot.Key getSnapshotKey(Result result, Scan scan) throws CrudException {
    Optional<Key> partitionKey = result.getPartitionKey();
    Optional<Key> clusteringKey = result.getClusteringKey();
    return new Snapshot.Key(
        scan.forNamespace().get(),
        scan.forTable().get(),
        partitionKey.orElseThrow(() -> new CrudException("can't get a snapshot key")),
        clusteringKey.orElse(null));
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }
}
