package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudRuntimeException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class Snapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(Snapshot.class);
  private final String id;
  private final Isolation isolation;
  private final SerializableStrategy strategy;
  private final Map<Key, Optional<TransactionResult>> readSet;
  private final Map<Scan, Optional<List<Key>>> scanSet;
  private final Map<Key, Put> writeSet;
  private final Map<Key, Delete> deleteSet;

  public Snapshot(String id) {
    this(id, Isolation.SNAPSHOT, SerializableStrategy.EXTRA_WRITE);
  }

  public Snapshot(String id, Isolation isolation, SerializableStrategy strategy) {
    this.id = id;
    this.isolation = isolation;
    this.strategy = strategy;
    this.readSet = new HashMap<>();
    this.scanSet = new HashMap<>();
    this.writeSet = new HashMap<>();
    this.deleteSet = new HashMap<>();
  }

  @VisibleForTesting
  Snapshot(
      String id,
      Isolation isolation,
      SerializableStrategy strategy,
      Map<Key, Optional<TransactionResult>> readSet,
      Map<Scan, Optional<List<Key>>> scanSet,
      Map<Key, Put> writeSet,
      Map<Key, Delete> deleteSet) {
    this.id = id;
    this.isolation = isolation;
    this.strategy = strategy;
    this.readSet = readSet;
    this.scanSet = scanSet;
    this.writeSet = writeSet;
    this.deleteSet = deleteSet;
  }

  @Nonnull
  public String getId() {
    return id;
  }

  @VisibleForTesting
  @Nonnull
  Isolation getIsolation() {
    return isolation;
  }

  public void put(Snapshot.Key key, Optional<TransactionResult> result) {
    readSet.put(key, result);
  }

  public void put(Scan scan, Optional<List<Key>> keys) {
    scanSet.put(scan, keys);
  }

  public void put(Snapshot.Key key, Put put) {
    deleteSet.remove(key);
    writeSet.put(key, put);
  }

  public void put(Snapshot.Key key, Delete delete) {
    writeSet.remove(key);
    deleteSet.put(key, delete);
  }

  public boolean containsKey(Snapshot.Key key) {
    return writeSet.containsKey(key) || deleteSet.containsKey(key) || readSet.containsKey(key);
  }

  public Optional<TransactionResult> get(Snapshot.Key key) {
    if (writeSet.containsKey(key)) {
      throw new CrudRuntimeException("reading already written data is not allowed");
    } else if (deleteSet.containsKey(key)) {
      return Optional.empty();
    } else if (readSet.containsKey(key)) {
      return readSet.get(key);
    }
    return Optional.empty();
  }

  public Optional<List<Key>> get(Scan scan) {
    if (isWriteSetOverlappedWith(scan)) {
      throw new CrudRuntimeException("reading already written data is not allowed");
    }
    if (scanSet.containsKey(scan)) {
      return scanSet.get(scan);
    }
    return Optional.empty();
  }

  public void to(MutationComposer composer) throws CommitConflictException {
    toSerializableWithExtraWrite(composer);

    writeSet.forEach(
        (key, value) -> {
          TransactionResult result =
              readSet.containsKey(key) ? readSet.get(key).orElse(null) : null;
          composer.add(value, result);
        });
    deleteSet.forEach(
        (key, value) -> {
          TransactionResult result =
              readSet.containsKey(key) ? readSet.get(key).orElse(null) : null;
          composer.add(value, result);
        });
  }

  private boolean isWriteSetOverlappedWith(Scan scan) {
    for (Map.Entry<Key, Put> entry : writeSet.entrySet()) {
      Put put = entry.getValue();
      if (!put.forNamespace().equals(scan.forNamespace())
          || !put.forTable().equals(scan.forTable())
          || !put.getPartitionKey().equals(scan.getPartitionKey())) {
        continue;
      }

      // If partition keys match and a primary key does not have a clustering key
      if (!put.getClusteringKey().isPresent()) {
        return true;
      }

      com.scalar.db.io.Key writtenKey = put.getClusteringKey().get();
      boolean isStartGiven = scan.getStartClusteringKey().isPresent();
      boolean isEndGiven = scan.getEndClusteringKey().isPresent();

      // If no range is specified, which means it scans the whole partition space
      if (!isStartGiven && !isEndGiven) {
        return true;
      }

      if (isStartGiven && isEndGiven) {
        com.scalar.db.io.Key startKey = scan.getStartClusteringKey().get();
        com.scalar.db.io.Key endKey = scan.getEndClusteringKey().get();
        // If startKey <= writtenKey <= endKey
        if ((scan.getStartInclusive() && writtenKey.equals(startKey))
            || (writtenKey.compareTo(startKey) > 0 && writtenKey.compareTo(endKey) < 0)
            || (scan.getEndInclusive() && writtenKey.equals(endKey))) {
          return true;
        }
      }

      if (isStartGiven && !isEndGiven) {
        com.scalar.db.io.Key startKey = scan.getStartClusteringKey().get();
        // If startKey <= writtenKey
        if ((scan.getStartInclusive() && startKey.equals(writtenKey))
            || writtenKey.compareTo(startKey) > 0) {
          return true;
        }
      }

      if (!isStartGiven) {
        com.scalar.db.io.Key endKey = scan.getEndClusteringKey().get();
        // If writtenKey <= endKey
        if ((scan.getEndInclusive() && writtenKey.equals(endKey))
            || writtenKey.compareTo(endKey) < 0) {
          return true;
        }
      }
    }
    return false;
  }

  @VisibleForTesting
  void toSerializableWithExtraWrite(MutationComposer composer) throws CommitConflictException {
    if (isolation != Isolation.SERIALIZABLE || strategy != SerializableStrategy.EXTRA_WRITE) {
      return;
    }

    for (Map.Entry<Key, Optional<TransactionResult>> entry : readSet.entrySet()) {
      Key key = entry.getKey();
      if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
        continue;
      }

      if (entry.getValue().isPresent() && composer instanceof PrepareMutationComposer) {
        // For existing records, convert a read set into a write set for Serializable. This needs to
        // be done in only prepare phase because the records are treated as written afterwards.
        Put put =
            new Put(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                .withConsistency(Consistency.LINEARIZABLE)
                .forNamespace(key.getNamespace())
                .forTable(key.getTable());
        writeSet.put(entry.getKey(), put);
      } else {
        // For non-existing records, special care is needed to guarantee Serializable. The records
        // are treated as not existed explicitly by preparing DELETED records so that conflicts can
        // be properly detected and handled. The records will be deleted in commit time by
        // rollforwad since the records are marked as DELETED or in recovery time by rollback since
        // the previous records are empty.
        Get get =
            new Get(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                .withConsistency(Consistency.LINEARIZABLE)
                .forNamespace(key.getNamespace())
                .forTable(key.getTable());
        composer.add(get, null);
      }
    }

    for (Map.Entry<Scan, Optional<List<Key>>> entry : scanSet.entrySet()) {
      // if there is a scan and a write in a transaction
      if (entry.getValue().isPresent() && !writeSet.isEmpty()) {
        throwExceptionDueToPotentialAntiDependency();
      }
    }
  }

  void toSerializableWithExtraRead(DistributedStorage storage)
      throws ExecutionException, CommitConflictException {
    if (!isExtraReadEnabled()) {
      return;
    }

    // Read set by scan is re-validated to check if there is no anti-dependency
    Set<Key> validatedReadSetByScan = new HashSet<>();
    for (Map.Entry<Scan, Optional<List<Key>>> entry : scanSet.entrySet()) {
      Set<TransactionResult> currentReadSetByScan = new HashSet<>();
      Scanner scanner = null;
      try {
        scanner = storage.scan(entry.getKey());
        for (Result result : scanner) {
          TransactionResult transactionResult = new TransactionResult(result);
          // Ignore records that this transaction has prepared (and that are in the write set)
          if (transactionResult.getId().equals(id)) {
            continue;
          }
          currentReadSetByScan.add(transactionResult);
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

      for (Key key : entry.getValue().get()) {
        if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
          continue;
        }
        // Check if read records are not changed
        if (!currentReadSetByScan.contains(readSet.get(key).get())) {
          throwExceptionDueToAntiDependency();
        }
        validatedReadSetByScan.add(key);
      }

      // Check if the size of a read set by scan is not changed
      if (currentReadSetByScan.size() != validatedReadSetByScan.size()) {
        throwExceptionDueToAntiDependency();
      }
    }

    // Read set by get is re-validated to check if there is no anti-dependency
    for (Map.Entry<Key, Optional<TransactionResult>> entry : readSet.entrySet()) {
      Key key = entry.getKey();
      if (writeSet.containsKey(key)
          || deleteSet.containsKey(key)
          || validatedReadSetByScan.contains(key)) {
        continue;
      }

      Get get =
          new Get(key.getPartitionKey(), key.getClusteringKey().orElse(null))
              .withConsistency(Consistency.LINEARIZABLE)
              .forNamespace(key.getNamespace())
              .forTable(key.getTable());

      Optional<TransactionResult> result = storage.get(get).map(TransactionResult::new);
      // Check if a read record is not changed
      if (!result.equals(entry.getValue())) {
        throwExceptionDueToAntiDependency();
      }
    }
  }

  private void throwExceptionDueToPotentialAntiDependency() throws CommitConflictException {
    LOGGER.warn(
        "reading empty records might cause write skew anomaly so aborting the transaction for safety.");
    throw new CommitConflictException(
        "reading empty records might cause write skew anomaly so aborting the transaction for safety.");
  }

  private void throwExceptionDueToAntiDependency() throws CommitConflictException {
    LOGGER.warn("Anti-dependency found. Aborting the transaction.");
    throw new CommitConflictException("Anti-dependency found. Aborting the transaction.");
  }

  private boolean isExtraReadEnabled() {
    return isolation == Isolation.SERIALIZABLE && strategy == SerializableStrategy.EXTRA_READ;
  }

  public boolean isPreCommitValidationRequired() {
    return isExtraReadEnabled();
  }

  @Immutable
  static final class Key implements Comparable<Key> {
    private final String namespace;
    private final String table;
    private final com.scalar.db.io.Key partitionKey;
    private final Optional<com.scalar.db.io.Key> clusteringKey;

    public Key(Get get) {
      this((Operation) get);
    }

    public Key(Put put) {
      this((Operation) put);
    }

    public Key(Delete delete) {
      this((Operation) delete);
    }

    public Key(
        String namespace,
        String table,
        com.scalar.db.io.Key partitionKey,
        @Nullable com.scalar.db.io.Key clusteringKey) {
      this.namespace = namespace;
      this.table = table;
      this.partitionKey = partitionKey;
      this.clusteringKey = Optional.ofNullable(clusteringKey);
    }

    private Key(Operation operation) {
      namespace = operation.forNamespace().get();
      table = operation.forTable().get();
      partitionKey = operation.getPartitionKey();
      clusteringKey = operation.getClusteringKey();
    }

    public String getNamespace() {
      return namespace;
    }

    public String getTable() {
      return table;
    }

    public com.scalar.db.io.Key getPartitionKey() {
      return partitionKey;
    }

    public Optional<com.scalar.db.io.Key> getClusteringKey() {
      return clusteringKey;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, table, partitionKey, clusteringKey);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Snapshot.Key)) {
        return false;
      }
      Key another = (Key) o;
      return this.namespace.equals(another.namespace)
          && this.table.equals(another.table)
          && this.partitionKey.equals(another.partitionKey)
          && this.clusteringKey.equals(another.clusteringKey);
    }

    @Override
    public int compareTo(Key o) {
      return ComparisonChain.start()
          .compare(this.namespace, o.namespace)
          .compare(this.table, o.table)
          .compare(this.partitionKey, o.partitionKey)
          .compare(
              this.clusteringKey.orElse(null),
              o.clusteringKey.orElse(null),
              Ordering.natural().nullsFirst())
          .result();
    }
  }
}
