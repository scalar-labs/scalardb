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
import com.scalar.db.exception.transaction.CrudRuntimeException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
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
    Optional<TransactionResult> result;
    Snapshot.Key key = new Snapshot.Key(get);

    result = snapshot.get(key);
    if (result.isPresent()) {
      return Optional.of(result.get());
    }

    result = getFromStorage(get);
    if (!result.isPresent()) {
      snapshot.put(key, result);
      return Optional.empty();
    }

    if (result.get().isCommitted()) {
      snapshot.put(key, result);
      return Optional.of(result.get());
    }
    throw new UncommittedRecordException(result.get(), "this record needs recovery");
  }

  public List<Result> scan(Scan scan) throws CrudException {
    List<Result> results = new ArrayList<>();
    List<Snapshot.Key> keys;

    keys = snapshot.get(scan);
    if (!keys.isEmpty()) {
      keys.forEach(key -> snapshot.get(key).ifPresent(r -> results.add(r)));
      return results;
    }

    keys = new ArrayList<>();
    for (Result r : getFromStorage(scan)) {
      TransactionResult result = new TransactionResult(r);
      if (!result.isCommitted()) {
        throw new UncommittedRecordException(result, "the record needs recovery");
      }

      Snapshot.Key key =
          getSnapshotKey(r, scan)
              .orElseThrow(() -> new CrudRuntimeException("can't get a snapshot key"));

      if (snapshot.get(key).isPresent()) {
        result = snapshot.get(key).get();
      }

      snapshot.put(key, Optional.of(result));
      keys.add(key);
      results.add(result);
    }
    snapshot.put(scan, keys);

    return results;
  }

  public void put(Put put) {
    snapshot.put(new Snapshot.Key(put), put);
  }

  public void delete(Delete delete) {
    snapshot.put(new Snapshot.Key(delete), delete);
  }

  private Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      get.withConsistency(Consistency.LINEARIZABLE);
      return storage.get(get).map(r -> new TransactionResult(r));
    } catch (ExecutionException e) {
      throw new CrudException("get failed.", e);
    }
  }

  private Scanner getFromStorage(Scan scan) throws CrudException {
    try {
      scan.withConsistency(Consistency.LINEARIZABLE);
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException("scan failed.", e);
    }
  }

  private Optional<Snapshot.Key> getSnapshotKey(Result result, Scan scan) {
    Optional<Key> partitionKey = result.getPartitionKey();
    Optional<Key> clusteringKey = result.getClusteringKey();
    if (!partitionKey.isPresent() || !clusteringKey.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(
        new Snapshot.Key(
            scan.forNamespace().get(),
            scan.forTable().get(),
            partitionKey.get(),
            clusteringKey.get()));
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }
}
