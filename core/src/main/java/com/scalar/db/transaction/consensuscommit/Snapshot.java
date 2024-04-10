package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.PrimaryKey;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.util.ScalarDbUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class Snapshot {
  private static final Logger logger = LoggerFactory.getLogger(Snapshot.class);
  private final String id;
  private final Isolation isolation;
  private final SerializableStrategy strategy;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final ConcurrentMap<PrimaryKey, Optional<TransactionResult>> readSet;
  private final Map<Scan, List<PrimaryKey>> scanSet;
  private final Map<PrimaryKey, Put> writeSet;
  private final Map<PrimaryKey, Delete> deleteSet;

  public Snapshot(
      String id,
      Isolation isolation,
      SerializableStrategy strategy,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.id = id;
    this.isolation = isolation;
    this.strategy = strategy;
    this.tableMetadataManager = tableMetadataManager;
    this.parallelExecutor = parallelExecutor;
    readSet = new ConcurrentHashMap<>();
    scanSet = new HashMap<>();
    writeSet = new HashMap<>();
    deleteSet = new HashMap<>();
  }

  @VisibleForTesting
  Snapshot(
      String id,
      Isolation isolation,
      SerializableStrategy strategy,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      ConcurrentMap<PrimaryKey, Optional<TransactionResult>> readSet,
      Map<Scan, List<PrimaryKey>> scanSet,
      Map<PrimaryKey, Put> writeSet,
      Map<PrimaryKey, Delete> deleteSet) {
    this.id = id;
    this.isolation = isolation;
    this.strategy = strategy;
    this.tableMetadataManager = tableMetadataManager;
    this.parallelExecutor = parallelExecutor;
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

  // Although this class is not thread-safe, this method is actually thread-safe because the readSet
  // is a concurrent map
  public void put(PrimaryKey key, Optional<TransactionResult> result) {
    readSet.put(key, result);
  }

  public void put(Scan scan, List<PrimaryKey> keys) {
    scanSet.put(scan, keys);
  }

  public void put(PrimaryKey key, Put put) {
    if (deleteSet.containsKey(key)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_WRITING_ALREADY_DELETED_DATA_NOT_ALLOWED.buildMessage());
    }
    if (writeSet.containsKey(key)) {
      // merge the previous put in the write set and the new put
      Put originalPut = writeSet.get(key);
      put.getColumns().values().forEach(originalPut::withValue);
    } else {
      writeSet.put(key, put);
    }
  }

  public void put(PrimaryKey key, Delete delete) {
    writeSet.remove(key);
    deleteSet.put(key, delete);
  }

  public boolean containsKeyInReadSet(PrimaryKey key) {
    return readSet.containsKey(key);
  }

  public Optional<TransactionResult> getFromReadSet(PrimaryKey key) {
    return readSet.getOrDefault(key, Optional.empty());
  }

  public List<Put> getPutsInWriteSet() {
    return new ArrayList<>(writeSet.values());
  }

  public List<Delete> getDeletesInDeleteSet() {
    return new ArrayList<>(deleteSet.values());
  }

  public Optional<TransactionResult> get(PrimaryKey key) throws CrudException {
    if (deleteSet.containsKey(key)) {
      return Optional.empty();
    } else if (readSet.containsKey(key)) {
      if (writeSet.containsKey(key)) {
        // merge the result in the read set and the put in the write set
        return Optional.of(
            new TransactionResult(
                new MergedResult(readSet.get(key), writeSet.get(key), getTableMetadata(key))));
      } else {
        return readSet.get(key);
      }
    }
    throw new IllegalArgumentException(
        CoreError.CONSENSUS_COMMIT_GETTING_DATA_NEITHER_IN_READ_SET_NOR_DELETE_SET_NOT_ALLOWED
            .buildMessage());
  }

  private TableMetadata getTableMetadata(PrimaryKey key) throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(
              key.getNamespaceName(), key.getTableName());
      if (metadata == null) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(
                ScalarDbUtils.getFullTableName(key.getNamespaceName(), key.getTableName())));
      }
      return metadata.getTableMetadata();
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, id);
    }
  }

  private TableMetadata getTableMetadata(Scan scan) throws ExecutionException {
    TransactionTableMetadata metadata = tableMetadataManager.getTransactionTableMetadata(scan);
    if (metadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(scan.forFullTableName().get()));
    }
    return metadata.getTableMetadata();
  }

  public Optional<List<PrimaryKey>> get(Scan scan) {
    if (scanSet.containsKey(scan)) {
      return Optional.ofNullable(scanSet.get(scan));
    }
    return Optional.empty();
  }

  public void verify(Scan scan) {
    if (ScalarDbUtils.isMutationSetOverlappedWith(scan, scanSet.get(scan), writeSet)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_READING_ALREADY_WRITTEN_DATA_NOT_ALLOWED.buildMessage());
    }
  }

  public void to(MutationComposer composer)
      throws ExecutionException, PreparationConflictException {
    toSerializableWithExtraWrite(composer);

    for (Entry<PrimaryKey, Put> entry : writeSet.entrySet()) {
      TransactionResult result =
          readSet.containsKey(entry.getKey()) ? readSet.get(entry.getKey()).orElse(null) : null;
      composer.add(entry.getValue(), result);
    }
    for (Entry<PrimaryKey, Delete> entry : deleteSet.entrySet()) {
      TransactionResult result =
          readSet.containsKey(entry.getKey()) ? readSet.get(entry.getKey()).orElse(null) : null;
      composer.add(entry.getValue(), result);
    }
  }

  @VisibleForTesting
  void toSerializableWithExtraWrite(MutationComposer composer)
      throws ExecutionException, PreparationConflictException {
    if (isolation != Isolation.SERIALIZABLE || strategy != SerializableStrategy.EXTRA_WRITE) {
      return;
    }

    for (Map.Entry<PrimaryKey, Optional<TransactionResult>> entry : readSet.entrySet()) {
      PrimaryKey key = entry.getKey();
      if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
        continue;
      }

      if (entry.getValue().isPresent() && composer instanceof PrepareMutationComposer) {
        // For existing records, convert a read set into a write set for Serializable. This needs to
        // be done in only prepare phase because the records are treated as written afterwards.
        Put put =
            new Put(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                .withConsistency(Consistency.LINEARIZABLE)
                .forNamespace(key.getNamespaceName())
                .forTable(key.getTableName());
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
                .forNamespace(key.getNamespaceName())
                .forTable(key.getTableName());
        composer.add(get, null);
      }
    }

    // if there is a scan and a write in a transaction
    if (!scanSet.isEmpty() && !writeSet.isEmpty()) {
      throwExceptionDueToPotentialAntiDependency();
    }
  }

  @VisibleForTesting
  void toSerializableWithExtraRead(DistributedStorage storage)
      throws ExecutionException, ValidationConflictException {
    if (!isExtraReadEnabled()) {
      return;
    }

    List<ParallelExecutorTask> tasks = new ArrayList<>();

    // Read set by scan is re-validated to check if there is no anti-dependency
    for (Map.Entry<Scan, List<PrimaryKey>> entry : scanSet.entrySet()) {
      tasks.add(
          () -> {
            Map<PrimaryKey, TransactionResult> currentReadMap = new HashMap<>();
            Set<PrimaryKey> validatedReadSet = new HashSet<>();
            Scanner scanner = null;
            try {
              Scan scan = entry.getKey();
              // only get tx_id and tx_version columns because we use only them to compare
              scan.clearProjections();
              scan.withProjection(Attribute.ID).withProjection(Attribute.VERSION);
              ScalarDbUtils.addProjectionsForKeys(scan, getTableMetadata(scan));
              scanner = storage.scan(scan);
              for (Result result : scanner) {
                TransactionResult transactionResult = new TransactionResult(result);
                // Ignore records that this transaction has prepared (and that are in the write set)
                if (transactionResult.getId() != null && transactionResult.getId().equals(id)) {
                  continue;
                }
                currentReadMap.put(new PrimaryKey(scan, result), transactionResult);
              }
            } finally {
              if (scanner != null) {
                try {
                  scanner.close();
                } catch (IOException e) {
                  logger.warn("Failed to close the scanner", e);
                }
              }
            }

            for (PrimaryKey key : entry.getValue()) {
              if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
                continue;
              }
              // Check if read records are not changed
              TransactionResult latestResult = currentReadMap.get(key);
              if (isChanged(Optional.of(latestResult), readSet.get(key))) {
                throwExceptionDueToAntiDependency();
              }
              validatedReadSet.add(key);
            }

            // Check if the size of a read set by scan is not changed
            if (currentReadMap.size() != validatedReadSet.size()) {
              throwExceptionDueToAntiDependency();
            }
          });
    }

    // Calculate read set validated by scan
    Set<PrimaryKey> validatedReadSetByScan = new HashSet<>();
    for (List<PrimaryKey> values : scanSet.values()) {
      validatedReadSetByScan.addAll(values);
    }

    // Read set by get is re-validated to check if there is no anti-dependency
    for (Map.Entry<PrimaryKey, Optional<TransactionResult>> entry : readSet.entrySet()) {
      PrimaryKey key = entry.getKey();
      if (writeSet.containsKey(key)
          || deleteSet.containsKey(key)
          || validatedReadSetByScan.contains(key)) {
        continue;
      }

      tasks.add(
          () -> {
            // only get tx_id and tx_version columns because we use only them to compare
            Get get =
                new Get(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                    .withProjection(Attribute.ID)
                    .withProjection(Attribute.VERSION)
                    .withConsistency(Consistency.LINEARIZABLE)
                    .forNamespace(key.getNamespaceName())
                    .forTable(key.getTableName());

            Optional<TransactionResult> latestResult = storage.get(get).map(TransactionResult::new);
            // Check if a read record is not changed
            if (isChanged(latestResult, entry.getValue())) {
              throwExceptionDueToAntiDependency();
            }
          });
    }

    parallelExecutor.validate(tasks, getId());
  }

  private boolean isChanged(
      Optional<TransactionResult> latestResult, Optional<TransactionResult> result) {
    if (latestResult.isPresent() != result.isPresent()) {
      return true;
    }
    if (!latestResult.isPresent()) {
      return false;
    }
    return !Objects.equals(latestResult.get().getId(), result.get().getId())
        || latestResult.get().getVersion() != result.get().getVersion();
  }

  private void throwExceptionDueToPotentialAntiDependency() throws PreparationConflictException {
    throw new PreparationConflictException(
        CoreError.CONSENSUS_COMMIT_READING_EMPTY_RECORDS_IN_EXTRA_WRITE.buildMessage(), id);
  }

  private void throwExceptionDueToAntiDependency() throws ValidationConflictException {
    throw new ValidationConflictException(
        CoreError.CONSENSUS_COMMIT_ANTI_DEPENDENCY_FOUND_IN_EXTRA_READ.buildMessage(), id);
  }

  private boolean isExtraReadEnabled() {
    return isolation == Isolation.SERIALIZABLE && strategy == SerializableStrategy.EXTRA_READ;
  }

  public boolean isValidationRequired() {
    return isExtraReadEnabled();
  }
}
