package com.scalar.database.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.database.api.Consistency;
import com.scalar.database.api.Delete;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.Get;
import com.scalar.database.api.Put;
import com.scalar.database.api.Result;
import com.scalar.database.api.Scan;
import com.scalar.database.api.Scanner;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.transaction.CrudException;
import com.scalar.database.exception.transaction.CrudRuntimeException;
import com.scalar.database.exception.transaction.UncommittedRecordException;
import com.scalar.database.io.Key;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Perform CRUD operations on {@link DistributedStorage} */
@ThreadSafe
public class CrudHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final Snapshot snapshot;

  public CrudHandler(DistributedStorage storage, Snapshot snapshot) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
  }

  /**
   * Returns a {@link Result} as the result of performing a {@link Get}
   *
   * @param get the {@code Get} to be performed
   * @return a {@link Result} as the result of performing a {@link Get}
   * @throws CrudException
   */
  public Optional<Result> get(Get get) throws CrudException {
    Optional<TransactionResult> result;
    Snapshot.Key key = new Snapshot.Key(get);

    result = snapshot.get(key);
    if (result.isPresent()) {
      return Optional.of(result.get());
    }

    result = getFromStorage(get);
    if (!result.isPresent()) {
      return Optional.empty();
    }

    if (result.get().isCommitted()) {
      snapshot.put(key, result.get());
      return Optional.of(result.get());
    }
    throw new UncommittedRecordException(result.get(), "this record needs recovery");
  }

  /**
   * Returns a list of {@link Result}s as the result of performing a {@link Scan}
   *
   * @param scan the {@code Scan} to be performed
   * @return a list of {@link Result}s as the result of performing a {@link Scan}
   * @throws CrudException
   */
  public List<Result> scan(Scan scan) throws CrudException {
    // NOTICE : scan needs to always look at storage first since no primary key is specified
    List<Result> results = new ArrayList<>();
    List<TransactionResult> uncommitted = new ArrayList<>();

    for (Result r : getFromStorage(scan)) {
      TransactionResult result = new TransactionResult(r);
      if (!result.isCommitted()) {
        uncommitted.add(result);
        continue;
      }
      results.add(result);
    }
    if (uncommitted.size() > 0) {
      throw new UncommittedRecordException(uncommitted, "these records need recovery");
    }

    // update snapshots
    results.forEach(
        r -> {
          Snapshot.Key key =
              getSnapshotKey(r, scan)
                  .orElseThrow(() -> new CrudRuntimeException("can't get a snapshot key"));

          if (snapshot.get(key).isPresent()) {
            LOGGER.warn("scanned records are already in snapshot. overwriting snapshot...");
          }
          snapshot.put(key, (TransactionResult) r);
        });

    return results;
  }

  /**
   * Performs a {@link Put} operation on the {@link Snapshot}
   *
   * @param put the {@code Put} to be performed
   */
  public void put(Put put) {
    snapshot.put(new Snapshot.Key(put), put);
  }

  /**
   * Performs a {@link Delete} operation on the {@link Snapshot}
   *
   * @param delete the {@code Delete} to be performed
   */
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
