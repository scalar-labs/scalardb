package com.scalar.database.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.scalar.database.api.Delete;
import com.scalar.database.api.Get;
import com.scalar.database.api.Isolation;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.exception.transaction.CrudRuntimeException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A snapshot used to do snapshot isolation. It can be changed to serializable if all the reads are
 * converted to writes to avoid write skew and read only anomalies. From a performance perspective
 * making it serializable should be avoided if possible as writes come with heavier operations.
 */
@ThreadSafe
public class Snapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(Snapshot.class);
  private final String id;
  private final Isolation isolation;
  private final Map<Key, TransactionResult> readSet;
  private final Map<Key, Put> writeSet;
  private final Map<Key, Delete> deleteSet;

  /**
   * Constructs a {@code Snapshot} with the specified id
   *
   * @param id
   */
  public Snapshot(String id) {
    this(id, Isolation.SNAPSHOT);
  }

  /**
   * Constructs a {@code Snapshot} with the specified id and {@link Isolation} level
   *
   * @param id
   * @param isolation an {@link Isolation}
   */
  public Snapshot(String id, Isolation isolation) {
    this.id = id;
    this.isolation = isolation;
    this.readSet = new ConcurrentHashMap<>();
    this.writeSet = new ConcurrentHashMap<>();
    this.deleteSet = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  Snapshot(
      String id,
      Isolation isolation,
      Map<Key, TransactionResult> readSet,
      Map<Key, Put> writeSet,
      Map<Key, Delete> deleteSet) {
    this.id = id;
    this.isolation = isolation;
    this.readSet = readSet;
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

  /**
   * Associate the specified {@link Key} with the {@link TransactionResult} in the {@code Snapshot}
   *
   * @param key a {@link Key}
   * @param result a {@link TransactionResult}
   */
  public void put(Snapshot.Key key, TransactionResult result) {
    readSet.put(key, result);
  }

  /**
   * Associate the specified {@link Key} with the {@link Put} in the {@code Snapshot}
   *
   * @param key a {@link Key}
   * @param put a {@link Put}
   */
  public void put(Snapshot.Key key, Put put) {
    writeSet.put(key, put);
  }

  /**
   * Associate the specified {@link Key} with the {@link Delete} in the {@code Snapshot}
   *
   * @param key a {@link Key}
   * @param delete a {@link Delete}
   */
  public void put(Snapshot.Key key, Delete delete) {
    deleteSet.put(key, delete);
  }

  /**
   * Returns the {@link TransactionResult} associated with the specified {@link Key} in the {@code
   * Snapshot}
   *
   * @param key a {@link Key}
   * @return a {@link Optional} with value {@link TransactionResult}
   */
  public Optional<TransactionResult> get(Snapshot.Key key) {
    if (writeSet.containsKey(key)) {
      throw new CrudRuntimeException("reading already written data is not allowed");
    } else if (readSet.containsKey(key)) {
      return Optional.of(readSet.get(key));
    }
    return Optional.empty();
  }

  public void to(MutationComposer composer) {
    if ((composer instanceof PrepareMutationComposer) && isolation == Isolation.SERIALIZABLE) {
      toSerializable();
    }

    writeSet
        .entrySet()
        .forEach(
            e -> {
              composer.add(e.getValue(), readSet.get(e.getKey()));
            });
    deleteSet
        .entrySet()
        .forEach(
            e -> {
              composer.add(e.getValue(), readSet.get(e.getKey()));
            });
  }

  @VisibleForTesting
  void toSerializable() {
    // TODO: scan might cause anomalies since it might cause phantoms ?
    readSet
        .entrySet()
        .forEach(
            e -> {
              Key key = e.getKey();
              if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
                return;
              }

              Put put =
                  new Put(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                      .forNamespace(key.getNamespace())
                      .forTable(key.getTable());
              // .withValues(readSet.get(key).getValues().values());
              writeSet.put(e.getKey(), put);
            });
  }

  @Immutable
  static final class Key implements Comparable<Key> {
    private final String namespace;
    private final String table;
    private final com.scalar.database.io.Key partitionKey;
    private final Optional<com.scalar.database.io.Key> clusteringKey;

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
        com.scalar.database.io.Key partitionKey,
        com.scalar.database.io.Key clusteringKey) {
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

    public com.scalar.database.io.Key getPartitionKey() {
      return partitionKey;
    }

    public Optional<com.scalar.database.io.Key> getClusteringKey() {
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
