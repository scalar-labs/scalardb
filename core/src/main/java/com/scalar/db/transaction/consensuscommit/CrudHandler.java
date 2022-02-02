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
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CrudHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final Snapshot snapshot;
  private final RecoveryHandler recovery;
  private final TransactionalTableMetadataManager tableMetadataManager;

  public CrudHandler(
      DistributedStorage storage,
      Snapshot snapshot,
      RecoveryHandler recovery,
      TransactionalTableMetadataManager tableMetadataManager) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
    this.recovery = checkNotNull(recovery);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
  }

  public Optional<Result> get(Get get) throws CrudException {
    Optional<TransactionResult> result;
    Snapshot.Key key = new Snapshot.Key(get);

    if (snapshot.containsKeyInReadSet(key)) {
      return snapshot.get(key).map(r -> r);
    }

    result = getFromStorage(get);
    if (!result.isPresent() || result.get().isCommitted()) {
      snapshot.put(key, result);
      return snapshot.get(key).map(r -> r);
    }

    // lazy recovery
    result = lazyRecovery(get, result.get());
    snapshot.put(key, result);
    return snapshot.get(key).map(r -> r);
  }

  public List<Result> scan(Scan scan) throws CrudException {
    List<Result> results = new ArrayList<>();

    Optional<List<Snapshot.Key>> keysInSnapshot = snapshot.get(scan);
    if (keysInSnapshot.isPresent()) {
      keysInSnapshot.get().forEach(key -> snapshot.get(key).ifPresent(results::add));
      return results;
    }

    List<Snapshot.Key> keys = new ArrayList<>();
    Scanner scanner = null;
    try {
      scanner = getFromStorage(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);

        if (!result.isCommitted()) {
          // lazy recovery
          Optional<TransactionResult> recoveredResult = lazyRecovery(scan, result);
          if (!recoveredResult.isPresent()) {
            // indicates the record is deleted in another transaction. skip it
            continue;
          }
          result = recoveredResult.get();
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
          LOGGER.warn("failed to close the scanner", e);
        }
      }
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
      // only get after image columns
      get.clearProjections();
      LinkedHashSet<String> afterImageColumnNames =
          tableMetadataManager.getTransactionalTableMetadata(get).getAfterImageColumnNames();
      get.withProjections(afterImageColumnNames);

      get.withConsistency(Consistency.LINEARIZABLE);
      return storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException("get failed.", e);
    }
  }

  private Scanner getFromStorage(Scan scan) throws CrudException {
    try {
      // only get after image columns
      scan.clearProjections();
      LinkedHashSet<String> afterImageColumnNames =
          tableMetadataManager.getTransactionalTableMetadata(scan).getAfterImageColumnNames();
      scan.withProjections(afterImageColumnNames);

      scan.withConsistency(Consistency.LINEARIZABLE);
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException("scan failed.", e);
    }
  }

  private Optional<TransactionResult> lazyRecovery(Selection selection, TransactionResult result)
      throws CrudException {
    try {
      return recovery.recover(selection, result);
    } catch (TransactionNotExpiredException e) {
      throw new CrudConflictException("read a record that the active transaction is updating", e);
    } catch (RecoveryException e) {
      throw new CrudException("recovering a record failed", e);
    }
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }
}
