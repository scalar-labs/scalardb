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
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import java.io.IOException;
import java.util.ArrayList;
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
  private final TransactionalTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final Map<Key, Optional<TransactionResult>> readSet;
  private final Map<Scan, List<Key>> scanSet;
  private final Map<Key, Put> writeSet;
  private final Map<Key, Delete> deleteSet;
  private final Map<String, TableSnapshot> tableSnapshots;

  public Snapshot(
      String id,
      Isolation isolation,
      SerializableStrategy strategy,
      TransactionalTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.id = id;
    this.isolation = isolation;
    this.strategy = strategy;
    this.tableMetadataManager = tableMetadataManager;
    this.parallelExecutor = parallelExecutor;
    readSet = new HashMap<>();
    scanSet = new HashMap<>();
    writeSet = new HashMap<>();
    deleteSet = new HashMap<>();
    tableSnapshots = new HashMap<>();
  }

  @VisibleForTesting
  Snapshot(
      String id,
      Isolation isolation,
      SerializableStrategy strategy,
      TransactionalTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      Map<Key, Optional<TransactionResult>> readSet,
      Map<Scan, List<Key>> scanSet,
      Map<Key, Put> writeSet,
      Map<Key, Delete> deleteSet,
      Map<String, TableSnapshot> tableSnapshots) {
    this.id = id;
    this.isolation = isolation;
    this.strategy = strategy;
    this.tableMetadataManager = tableMetadataManager;
    this.parallelExecutor = parallelExecutor;
    this.readSet = readSet;
    this.scanSet = scanSet;
    this.writeSet = writeSet;
    this.deleteSet = deleteSet;
    this.tableSnapshots = tableSnapshots;
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

  public void put(Key key, Selection selection, Optional<TransactionResult> result)
      throws CrudException {
    readSet.put(key, result);
    if (result.isPresent()) {
      // put data into a table snapshot
      getTableSnapshot(selection)
          .put(
              selection.getPartitionKey(),
              result.get().getClusteringKey(),
              result.get().getValues());
    }
  }

  public void put(Scan scan, List<Key> keys) {
    scanSet.put(scan, keys);
  }

  public void put(Key key, Put put) throws CrudException {
    if (deleteSet.containsKey(key)) {
      throw new IllegalArgumentException(
          "Currently, writing data in the delete set is not allowed");
    }
    if (writeSet.containsKey(key)) {
      // merge the previous put in the write set and the new put
      writeSet.get(key).withValues(put.getValues().values());
    } else {
      writeSet.put(key, put);
    }

    // put data into a table snapshot
    getTableSnapshot(put).put(put.getPartitionKey(), put.getClusteringKey(), put.getValues());
  }

  public void put(Key key, Delete delete) throws CrudException {
    writeSet.remove(key);
    deleteSet.put(key, delete);

    // delete data from a table snapshot
    getTableSnapshot(delete).delete(delete.getPartitionKey(), delete.getClusteringKey());
  }

  public boolean containsKeyInReadSet(Snapshot.Key key) {
    return readSet.containsKey(key);
  }

  public boolean containsKeyInScanSet(Scan scan) {
    return scanSet.containsKey(scan);
  }

  public Optional<Result> get(Get get) throws CrudException {
    // get data from a table snapshot
    return getTableSnapshot(get).get(get.getPartitionKey(), get.getClusteringKey());
  }

  public List<Result> scan(Scan scan) throws CrudException {
    // scan data from a table snapshot
    return getTableSnapshot(scan)
        .scan(
            scan.getPartitionKey(),
            scan.getStartClusteringKey(),
            scan.getStartInclusive(),
            scan.getEndClusteringKey(),
            scan.getEndInclusive(),
            scan.getOrderings(),
            scan.getLimit());
  }

  private TableSnapshot getTableSnapshot(Operation operation) throws CrudException {
    try {
      String fullTableName = operation.forFullTableName().get();
      if (!tableSnapshots.containsKey(fullTableName)) {
        TransactionalTableMetadata metadata =
            tableMetadataManager.getTransactionalTableMetadata(operation);
        tableSnapshots.put(fullTableName, new TableSnapshot(metadata.getTableMetadata()));
      }
      return tableSnapshots.get(fullTableName);
    } catch (ExecutionException e) {
      throw new CrudException("getting metadata failed", e);
    }
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

    // if there is a scan and a write in a transaction
    if (!scanSet.isEmpty() && !writeSet.isEmpty()) {
      throwExceptionDueToPotentialAntiDependency();
    }
  }

  void toSerializableWithExtraRead(DistributedStorage storage)
      throws ExecutionException, CommitConflictException {
    if (!isExtraReadEnabled()) {
      return;
    }

    List<ParallelExecutorTask> tasks = new ArrayList<>();

    // Read set by scan is re-validated to check if there is no anti-dependency
    for (Map.Entry<Scan, List<Key>> entry : scanSet.entrySet()) {
      tasks.add(
          () -> {
            Set<TransactionResult> currentReadSet = new HashSet<>();
            Set<Key> validatedReadSet = new HashSet<>();
            Scanner scanner = null;
            try {
              scanner = storage.scan(entry.getKey());
              for (Result result : scanner) {
                TransactionResult transactionResult = new TransactionResult(result);
                // Ignore records that this transaction has prepared (and that are in the write set)
                if (transactionResult.getId().equals(id)) {
                  continue;
                }
                currentReadSet.add(transactionResult);
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

            for (Key key : entry.getValue()) {
              if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
                continue;
              }
              // Check if read records are not changed
              if (readSet.get(key).isPresent()
                  && !currentReadSet.contains(readSet.get(key).get())) {
                throwExceptionDueToAntiDependency();
              }
              validatedReadSet.add(key);
            }

            // Check if the size of a read set by scan is not changed
            if (currentReadSet.size() != validatedReadSet.size()) {
              throwExceptionDueToAntiDependency();
            }
          });
    }

    // Calculate read set validated by scan
    Set<Key> validatedReadSetByScan = new HashSet<>();
    for (List<Key> values : scanSet.values()) {
      validatedReadSetByScan.addAll(values);
    }

    // Read set by get is re-validated to check if there is no anti-dependency
    for (Map.Entry<Key, Optional<TransactionResult>> entry : readSet.entrySet()) {
      Key key = entry.getKey();
      if (writeSet.containsKey(key)
          || deleteSet.containsKey(key)
          || validatedReadSetByScan.contains(key)) {
        continue;
      }

      tasks.add(
          () -> {
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
          });
    }

    parallelExecutor.validate(tasks);
  }

  private void throwExceptionDueToPotentialAntiDependency() throws CommitConflictException {
    throw new CommitConflictException(
        "reading empty records might cause write skew anomaly so aborting the transaction for safety.");
  }

  private void throwExceptionDueToAntiDependency() throws CommitConflictException {
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
