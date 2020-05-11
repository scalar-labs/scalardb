package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitRuntimeException;
import com.scalar.db.exception.transaction.CrudRuntimeException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
@ThreadSafe
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
    this.readSet = new ConcurrentHashMap<>();
    this.scanSet = new ConcurrentHashMap<>();
    this.writeSet = new ConcurrentHashMap<>();
    this.deleteSet = new ConcurrentHashMap<>();
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

  @Nonnull
  public Isolation getIsolation() {
    return isolation;
  }

  public void put(Snapshot.Key key, Optional<TransactionResult> result) {
    readSet.put(key, result);
  }

  public void put(Scan scan, Optional<List<Key>> keys) {
    scanSet.put(scan, keys);
  }

  public void put(Snapshot.Key key, Put put) {
    writeSet.put(key, put);
  }

  public void put(Snapshot.Key key, Delete delete) {
    deleteSet.put(key, delete);
  }

  public Optional<TransactionResult> get(Snapshot.Key key) {
    if (writeSet.containsKey(key)) {
      throw new CrudRuntimeException("reading already written data is not allowed");
    } else if (readSet.containsKey(key)) {
      return readSet.get(key);
    }
    return Optional.empty();
  }

  public Optional<List<Key>> get(Scan scan) {
    if (scanSet.containsKey(scan)) {
      return scanSet.get(scan);
    }
    return Optional.empty();
  }

  public void to(MutationComposer composer) {
    if ((composer instanceof PrepareMutationComposer)
        && isolation == Isolation.SERIALIZABLE
        && strategy == SerializableStrategy.EXTRA_WRITE) {
      toSerializable();
    }

    writeSet
        .entrySet()
        .forEach(
            e -> {
              TransactionResult result =
                  readSet.get(e.getKey()) == null ? null : readSet.get(e.getKey()).orElse(null);
              composer.add(e.getValue(), result);
            });
    deleteSet
        .entrySet()
        .forEach(
            e -> {
              TransactionResult result =
                  readSet.get(e.getKey()) == null ? null : readSet.get(e.getKey()).orElse(null);
              composer.add(e.getValue(), result);
            });
  }

  @VisibleForTesting
  void toSerializable() {
    readSet
        .entrySet()
        .forEach(
            e -> {
              Key key = e.getKey();
              if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
                return;
              }

              if (!e.getValue().isPresent()) {
                throw new CommitRuntimeException(
                    "reading empty records might cause write skew anomaly "
                        + "so aborting the transaction for safety.");
              }

              Put put =
                  new Put(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                      .withConsistency(Consistency.LINEARIZABLE)
                      .forNamespace(key.getNamespace())
                      .forTable(key.getTable());
              writeSet.put(e.getKey(), put);
            });

    scanSet
        .entrySet()
        .forEach(
            e -> {
              // if there is a scan and a write in a transaction
              if (e.getValue().isPresent() && !writeSet.isEmpty()) {
                throw new CommitRuntimeException(
                    "reading empty records might cause write skew anomaly "
                        + "so aborting the transaction for safety.");
              }
            });
  }

  void toSerializableWithExtraRead(DistributedStorage storage)
      throws ExecutionException, CommitConflictException {
    if (isolation != Isolation.SERIALIZABLE || strategy != SerializableStrategy.EXTRA_READ) {
      return;
    }

    // Read set by scan is re-validated to check if there is no anti-dependency
    Set<Key> validatedReadSetByScan = new HashSet<>();
    for (Map.Entry<Scan, Optional<List<Key>>> entry : scanSet.entrySet()) {
      Set<TransactionResult> currentReadSetByScan = new HashSet<>();
      for (Result result : storage.scan(entry.getKey())) {
        TransactionResult transactionResult = new TransactionResult(result);
        // Ignore records that this transaction has prepared (and that are in the write set)
        if (transactionResult.getId().equals(id)) {
          continue;
        }
        currentReadSetByScan.add(transactionResult);
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

      Optional<TransactionResult> result = storage.get(get).map(r -> new TransactionResult(r));
      // Check if a read record is not changed
      if (!result.equals(entry.getValue())) {
        throwExceptionDueToAntiDependency();
      }
    }
  }

  private void throwExceptionDueToAntiDependency() throws CommitConflictException {
    LOGGER.warn("Anti-dependency found. Aborting the transaction.");
    throw new CommitConflictException("Anti-dependency found. Aborting the transaction.");
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
        com.scalar.db.io.Key clusteringKey) {
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
          && ((this.clusteringKey == null && another.clusteringKey == null)
              || this.clusteringKey.equals(another.clusteringKey));
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
