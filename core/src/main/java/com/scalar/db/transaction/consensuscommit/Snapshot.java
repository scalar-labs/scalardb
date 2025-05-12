package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes.isImplicitPreReadEnabled;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes.isInsertModeEnabled;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.scalar.db.api.ConditionSetBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.ScanWithIndex;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.util.ScalarDbUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class Snapshot {
  private static final Logger logger = LoggerFactory.getLogger(Snapshot.class);
  private final String id;
  private final Isolation isolation;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final ConcurrentMap<Key, Optional<TransactionResult>> readSet;
  private final ConcurrentMap<Get, Optional<TransactionResult>> getSet;
  private final Map<Scan, LinkedHashMap<Key, TransactionResult>> scanSet;
  private final Map<Key, Put> writeSet;
  private final Map<Key, Delete> deleteSet;

  public Snapshot(
      String id,
      Isolation isolation,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.id = id;
    this.isolation = isolation;
    this.tableMetadataManager = tableMetadataManager;
    this.parallelExecutor = parallelExecutor;
    readSet = new ConcurrentHashMap<>();
    getSet = new ConcurrentHashMap<>();
    scanSet = new HashMap<>();
    writeSet = new HashMap<>();
    deleteSet = new HashMap<>();
  }

  @VisibleForTesting
  Snapshot(
      String id,
      Isolation isolation,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      ConcurrentMap<Key, Optional<TransactionResult>> readSet,
      ConcurrentMap<Get, Optional<TransactionResult>> getSet,
      Map<Scan, LinkedHashMap<Key, TransactionResult>> scanSet,
      Map<Key, Put> writeSet,
      Map<Key, Delete> deleteSet) {
    this.id = id;
    this.isolation = isolation;
    this.tableMetadataManager = tableMetadataManager;
    this.parallelExecutor = parallelExecutor;
    this.readSet = readSet;
    this.getSet = getSet;
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
  public void putIntoReadSet(Key key, Optional<TransactionResult> result) {
    readSet.put(key, result);
  }

  // Although this class is not thread-safe, this method is actually thread-safe because the getSet
  // is a concurrent map
  public void putIntoGetSet(Get get, Optional<TransactionResult> result) {
    getSet.put(get, result);
  }

  public void putIntoScanSet(Scan scan, LinkedHashMap<Key, TransactionResult> results) {
    scanSet.put(scan, results);
  }

  public void putIntoWriteSet(Key key, Put put) {
    if (deleteSet.containsKey(key)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_WRITING_ALREADY_DELETED_DATA_NOT_ALLOWED.buildMessage());
    }
    if (writeSet.containsKey(key)) {
      if (isInsertModeEnabled(put)) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_INSERTING_ALREADY_WRITTEN_DATA_NOT_ALLOWED.buildMessage());
      }

      // merge the previous put in the write set and the new put
      Put originalPut = writeSet.get(key);
      PutBuilder.BuildableFromExisting putBuilder = Put.newBuilder(originalPut);
      put.getColumns().values().forEach(putBuilder::value);

      // If the implicit pre-read is enabled for the new put, it should also be enabled for the
      // merged put. However, if the previous put is in insert mode, this doesn’t apply. This is
      // because, in insert mode, the read set is not used during the preparation phase. Therefore,
      // we only need to enable the implicit pre-read if the previous put is not in insert mode
      if (isImplicitPreReadEnabled(put) && !isInsertModeEnabled(originalPut)) {
        putBuilder.enableImplicitPreRead();
      }

      writeSet.put(key, putBuilder.build());
    } else {
      writeSet.put(key, put);
    }
  }

  public void putIntoDeleteSet(Key key, Delete delete) {
    Put put = writeSet.get(key);
    if (put != null) {
      if (isInsertModeEnabled(put)) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_DELETING_ALREADY_INSERTED_DATA_NOT_ALLOWED.buildMessage());
      }

      writeSet.remove(key);
    }

    deleteSet.put(key, delete);
  }

  public List<Put> getPutsInWriteSet() {
    return new ArrayList<>(writeSet.values());
  }

  public List<Delete> getDeletesInDeleteSet() {
    return new ArrayList<>(deleteSet.values());
  }

  public ReadWriteSets getReadWriteSets() {
    return new ReadWriteSets(id, readSet, writeSet.entrySet(), deleteSet.entrySet());
  }

  public boolean containsKeyInReadSet(Key key) {
    return readSet.containsKey(key);
  }

  public boolean containsKeyInGetSet(Get get) {
    return getSet.containsKey(get);
  }

  public Optional<TransactionResult> getResult(Key key) throws CrudException {
    Optional<TransactionResult> result = readSet.getOrDefault(key, Optional.empty());
    return mergeResult(key, result);
  }

  public Optional<TransactionResult> getResult(Key key, Get get) throws CrudException {
    Optional<TransactionResult> result = getSet.getOrDefault(get, Optional.empty());
    return mergeResult(key, result, get.getConjunctions());
  }

  public Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> getResults(Scan scan)
      throws CrudException {
    if (!scanSet.containsKey(scan)) {
      return Optional.empty();
    }
    return Optional.of(scanSet.get(scan));
  }

  private Optional<TransactionResult> mergeResult(Key key, Optional<TransactionResult> result)
      throws CrudException {
    if (deleteSet.containsKey(key)) {
      return Optional.empty();
    } else if (writeSet.containsKey(key)) {
      // merge the result in the read set and the put in the write set
      return Optional.of(
          new TransactionResult(
              new MergedResult(result, writeSet.get(key), getTableMetadata(key))));
    } else {
      return result;
    }
  }

  private Optional<TransactionResult> mergeResult(
      Key key, Optional<TransactionResult> result, Set<Conjunction> conjunctions)
      throws CrudException {
    return mergeResult(key, result)
        .filter(
            r ->
                // We need to apply conditions if it is a merged result because the transaction’s
                // write makes the record no longer match the conditions. Of course, we can just
                // return the result without checking the condition if there is no condition.
                !r.isMergedResult()
                    || conjunctions.isEmpty()
                    || ScalarDbUtils.columnsMatchAnyOfConjunctions(r.getColumns(), conjunctions));
  }

  private TableMetadata getTableMetadata(Key key) throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(key.getNamespace(), key.getTable());
      if (metadata == null) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(
                ScalarDbUtils.getFullTableName(key.getNamespace(), key.getTable())));
      }
      return metadata.getTableMetadata();
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, id);
    }
  }

  public void to(MutationComposer composer)
      throws ExecutionException, PreparationConflictException {
    for (Entry<Key, Put> entry : writeSet.entrySet()) {
      TransactionResult result =
          readSet.containsKey(entry.getKey()) ? readSet.get(entry.getKey()).orElse(null) : null;
      composer.add(entry.getValue(), result);
    }
    for (Entry<Key, Delete> entry : deleteSet.entrySet()) {
      TransactionResult result =
          readSet.containsKey(entry.getKey()) ? readSet.get(entry.getKey()).orElse(null) : null;
      composer.add(entry.getValue(), result);
    }
  }

  /**
   * Verifies that the scan does not overlap with the previous write or delete operations of the
   * same transaction to prevent incorrect results.
   *
   * <p>For instance, consider the following records in the database, where X is the partition key
   * and Y is the clustering key: R1(X=1, Y=1), R2(X=1, Y=2), R3(X=1, Y=3), and R4(X=1, Y=4).
   *
   * <p>If a transaction performs a write (insert) operation for a new record R5(X=1, Y=5) followed
   * by a scan operation with partition key X=1, the overlap check is crucial. Without this check,
   * the scan might return {R1, R2, R3, R4}, which is incorrect. The correct result should include
   * the newly inserted record, yielding {R1, R2, R3, R4, R5}.
   *
   * <p>Similarly, if a transaction deletes R1(X=1, Y=1) and then issues a scan operation with
   * partition key X=1 and a limit of 3, the result without the overlap check would incorrectly
   * return {R2, R3}. The correct result should be {R2, R3, R4}.
   *
   * <p>These examples illustrate basic cases, but the situation becomes more complex when
   * considering the ordering of results and limit constraints. Therefore, ScalarDB currently avoids
   * such overlaps instead of attempting to handle more intricate scenarios.
   *
   * <p>The use of scanned results, in addition to the specified scan range, is necessary because we
   * support arbitrary conditions in the WHERE clause of scan operations with Scan, ScanAll, and
   * ScanWithIndex. Specifically, without knowing the keys obtained in the actual scan, it is
   * impossible to determine whether the scanned results include any records that match the keys
   * from previous writes or deletes.
   *
   * @param scan the scan to be verified
   * @param results the results of the scan
   */
  public void verifyNoOverlap(Scan scan, Map<Snapshot.Key, TransactionResult> results) {
    if (isWriteSetOrDeleteSetOverlappedWith(scan, results)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_SCANNING_ALREADY_WRITTEN_OR_DELETED_DATA_NOT_ALLOWED
              .buildMessage());
    }
  }

  private boolean isWriteSetOrDeleteSetOverlappedWith(
      Scan scan, Map<Snapshot.Key, TransactionResult> results) {
    if (isDeleteSetOverlappedWith(results)) {
      return true;
    }

    if (scan instanceof ScanWithIndex) {
      return isWriteSetOverlappedWith((ScanWithIndex) scan, results);
    } else if (scan instanceof ScanAll) {
      return isWriteSetOverlappedWith((ScanAll) scan, results);
    } else {
      return isWriteSetOverlappedWith(scan, results);
    }
  }

  private boolean isDeleteSetOverlappedWith(Map<Snapshot.Key, TransactionResult> results) {
    for (Map.Entry<Key, Delete> entry : deleteSet.entrySet()) {
      if (results.containsKey(entry.getKey())) {
        return true;
      }
    }
    return false;
  }

  private boolean isWriteSetOverlappedWith(
      ScanWithIndex scanWithIndex, Map<Snapshot.Key, TransactionResult> results) {
    for (Map.Entry<Key, Put> entry : writeSet.entrySet()) {
      if (results.containsKey(entry.getKey())) {
        return true;
      }

      Put put = entry.getValue();
      if (!put.forNamespace().equals(scanWithIndex.forNamespace())
          || !put.forTable().equals(scanWithIndex.forTable())) {
        continue;
      }

      if (!areConjunctionsOverlapped(put, scanWithIndex)) {
        continue;
      }

      Map<String, Column<?>> columns = getAllColumns(put);
      Column<?> indexColumn = scanWithIndex.getPartitionKey().getColumns().get(0);
      String indexColumnName = indexColumn.getName();
      if (columns.containsKey(indexColumnName)
          && columns.get(indexColumnName).equals(indexColumn)) {
        return true;
      }
    }
    return false;
  }

  private boolean isWriteSetOverlappedWith(
      ScanAll scanAll, Map<Snapshot.Key, TransactionResult> results) {
    for (Map.Entry<Key, Put> entry : writeSet.entrySet()) {
      // We need to consider three cases here to prevent scan-after-write.
      //   1) A put operation overlaps the scan range regardless of the update (put) results.
      //   2) A put operation does not overlap the scan range as a result of the update.
      //   3) A put operation overlaps the scan range as a result of the update.
      // See the following examples. Assume that we have a table with two columns whose names are
      // "key" and "value" and two records in the table: (key=1, value=2) and (key=2, value=3).
      // Case 2 covers a transaction that puts (1, 4) and then scans "where value < 3". In this
      // case, there is no overlap, but we intentionally prohibit it due to the consistency and
      // simplicity of snapshot management. We can find case 2 using the scan results.
      // Case 3 covers a transaction that puts (2, 2) and then scans "where value < 3". In this
      // case, we cannot find the overlap using the scan results since the database is not updated
      // yet. Thus, we need to evaluate if the scan condition potentially matches put operations.

      // Check for cases 1 and 2
      if (results.containsKey(entry.getKey())) {
        return true;
      }

      // Check for case 3
      Put put = entry.getValue();
      if (!put.forNamespace().equals(scanAll.forNamespace())
          || !put.forTable().equals(scanAll.forTable())) {
        continue;
      }

      if (areConjunctionsOverlapped(put, scanAll)) {
        return true;
      }
    }
    return false;
  }

  private boolean isWriteSetOverlappedWith(
      Scan scan, Map<Snapshot.Key, TransactionResult> results) {
    for (Map.Entry<Key, Put> entry : writeSet.entrySet()) {
      if (results.containsKey(entry.getKey())) {
        return true;
      }

      Put put = entry.getValue();
      if (!put.forNamespace().equals(scan.forNamespace())
          || !put.forTable().equals(scan.forTable())
          || !put.getPartitionKey().equals(scan.getPartitionKey())) {
        continue;
      }

      if (!areConjunctionsOverlapped(put, scan)) {
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

  private boolean areConjunctionsOverlapped(Put put, Scan scan) {
    if (scan.getConjunctions().isEmpty()) {
      return true;
    }

    Map<String, Column<?>> columns = getAllColumns(put);
    return ScalarDbUtils.columnsMatchAnyOfConjunctions(columns, scan.getConjunctions());
  }

  private Map<String, Column<?>> getAllColumns(Put put) {
    Map<String, Column<?>> columns = new HashMap<>(put.getColumns());
    put.getPartitionKey().getColumns().forEach(column -> columns.put(column.getName(), column));
    put.getClusteringKey()
        .ifPresent(
            key -> key.getColumns().forEach(column -> columns.put(column.getName(), column)));
    return columns;
  }

  @VisibleForTesting
  void toSerializable(DistributedStorage storage)
      throws ExecutionException, ValidationConflictException {
    if (!isSerializable()) {
      return;
    }

    List<ParallelExecutorTask> tasks = new ArrayList<>();

    // Scan set is re-validated to check if there is no anti-dependency
    for (Map.Entry<Scan, LinkedHashMap<Key, TransactionResult>> entry : scanSet.entrySet()) {
      tasks.add(() -> validateScanResults(storage, entry.getKey(), entry.getValue()));
    }

    // Get set is re-validated to check if there is no anti-dependency
    for (Map.Entry<Get, Optional<TransactionResult>> entry : getSet.entrySet()) {
      Get get = entry.getKey();

      if (ScalarDbUtils.isSecondaryIndexSpecified(get, getTableMetadata(get))) {
        // For Get with index

        tasks.add(
            () -> {
              Optional<TransactionResult> originalResult = entry.getValue();

              // If this transaction or another transaction inserts records into the index range,
              // the Get with index operation may retrieve multiple records, which would result in
              // an IllegalArgumentException. Therefore, we use Scan with index instead.
              Scan scanWithIndex =
                  Scan.newBuilder()
                      .namespace(get.forNamespace().get())
                      .table(get.forTable().get())
                      .indexKey(get.getPartitionKey())
                      .whereOr(
                          get.getConjunctions().stream()
                              .map(
                                  c ->
                                      ConditionSetBuilder.andConditionSet(c.getConditions())
                                          .build())
                              .collect(Collectors.toSet()))
                      .consistency(get.getConsistency())
                      .attributes(get.getAttributes())
                      .build();

              LinkedHashMap<Key, TransactionResult> results = new LinkedHashMap<>(1);
              originalResult.ifPresent(r -> results.put(new Snapshot.Key(scanWithIndex, r), r));

              // Validate the result to check if there is no anti-dependency
              validateScanResults(storage, scanWithIndex, results);
            });
      } else {
        // For other Get

        Key key = new Key(get);
        if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
          continue;
        }

        tasks.add(
            () -> {
              Optional<TransactionResult> originalResult = entry.getValue();

              // Only get the tx_id column because we use only them to compare
              get.clearProjections();
              get.withProjection(Attribute.ID);

              // Check if a read record is not changed
              Optional<TransactionResult> latestResult =
                  storage.get(get).map(TransactionResult::new);
              if (isChanged(latestResult, originalResult)) {
                throwExceptionDueToAntiDependency();
              }
            });
      }
    }

    parallelExecutor.validate(tasks, getId());
  }

  /**
   * Validates the scan results to check if there is no anti-dependency.
   *
   * <p>This method scans the latest data and compares it with the scan results. If there is a
   * discrepancy, it means that the scan results are changed by another transaction. In this case,
   * an {@link ValidationConflictException} is thrown.
   *
   * <p>Since the validation is performed after the prepare-record phase, the scan might include
   * prepared records if the transaction has performed operations that affect the scan result. In
   * such cases, those prepared records can be safely ignored.
   *
   * <p>Note that this logic is based on the assumption that identical scans return results in the
   * same order, provided that the underlying data remains unchanged.
   *
   * @param storage a distributed storage
   * @param scan the scan to be validated
   * @param results the results of the scan
   * @throws ExecutionException if a storage operation fails
   * @throws ValidationConflictException if the scan results are changed by another transaction
   */
  private void validateScanResults(
      DistributedStorage storage, Scan scan, LinkedHashMap<Key, TransactionResult> results)
      throws ExecutionException, ValidationConflictException {
    Scanner scanner = null;
    try {
      // Only get tx_id and primary key columns because we use only them to compare
      scan.clearProjections();
      scan.withProjection(Attribute.ID);
      ScalarDbUtils.addProjectionsForKeys(scan, getTableMetadata(scan));

      if (scan.getLimit() == 0) {
        scanner = storage.scan(scan);
      } else {
        // Get a scanner without the limit if the scan has a limit
        scanner = storage.scan(Scan.newBuilder(scan).limit(0).build());
      }

      // Initialize the iterator for the latest scan results
      Optional<Result> latestResult = scanner.one();

      // Initialize the iterator for the original scan results
      Iterator<Entry<Key, TransactionResult>> originalResultIterator =
          results.entrySet().iterator();
      Entry<Key, TransactionResult> originalResultEntry =
          Iterators.getNext(originalResultIterator, null);

      // Compare the records of the iterators
      while (latestResult.isPresent() && originalResultEntry != null) {
        TransactionResult latestTxResult = new TransactionResult(latestResult.get());
        Key key = new Key(scan, latestTxResult);

        if (latestTxResult.getId() != null && latestTxResult.getId().equals(id)) {
          // The record is inserted/deleted/updated by this transaction

          // Skip the record of the latest scan results
          latestResult = scanner.one();

          if (originalResultEntry.getKey().equals(key)) {
            // The record is updated by this transaction

            // Skip the record of the original scan results
            originalResultEntry = Iterators.getNext(originalResultIterator, null);
          } else {
            // The record is inserted/deleted by this transaction
          }

          continue;
        }

        // Compare the records of the original scan results and the latest scan results
        if (!originalResultEntry.getKey().equals(key)) {
          // The record is inserted/deleted by another transaction
          throwExceptionDueToAntiDependency();
        }
        if (isChanged(latestTxResult, originalResultEntry.getValue())) {
          // The record is updated by another transaction
          throwExceptionDueToAntiDependency();
        }

        // Proceed to the next record
        latestResult = scanner.one();
        originalResultEntry = Iterators.getNext(originalResultIterator, null);
      }

      if (originalResultEntry != null) {
        // Some of the records of the scan results are deleted by another transaction
        throwExceptionDueToAntiDependency();
      }

      if (scan.getLimit() != 0 && results.size() == scan.getLimit()) {
        // We’ve already checked up to the limit, so no further checks are needed
        return;
      }

      // Check if there are any remaining records in the latest scan results
      while (latestResult.isPresent()) {
        TransactionResult latestTxResult = new TransactionResult(latestResult.get());

        if (latestTxResult.getId() != null && latestTxResult.getId().equals(id)) {
          // The record is inserted/deleted by this transaction

          // Skip the record
          latestResult = scanner.one();
        } else {
          // The record is inserted by another transaction
          throwExceptionDueToAntiDependency();
        }
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
  }

  private TableMetadata getTableMetadata(Operation operation) throws ExecutionException {
    TransactionTableMetadata metadata = tableMetadataManager.getTransactionTableMetadata(operation);
    if (metadata == null) {
      assert operation.forFullTableName().isPresent();
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(operation.forFullTableName().get()));
    }
    return metadata.getTableMetadata();
  }

  private boolean isChanged(
      Optional<TransactionResult> latestResult, Optional<TransactionResult> result) {
    if (latestResult.isPresent() != result.isPresent()) {
      return true;
    }
    if (!latestResult.isPresent()) {
      return false;
    }
    return isChanged(latestResult.get(), result.get());
  }

  private boolean isChanged(TransactionResult latestResult, TransactionResult result) {
    return !Objects.equals(latestResult.getId(), result.getId());
  }

  private void throwExceptionDueToAntiDependency() throws ValidationConflictException {
    throw new ValidationConflictException(
        CoreError.CONSENSUS_COMMIT_ANTI_DEPENDENCY_FOUND.buildMessage(), id);
  }

  private boolean isSerializable() {
    return isolation == Isolation.SERIALIZABLE;
  }

  public boolean isValidationRequired() {
    return isSerializable();
  }

  @Immutable
  public static final class Key implements Comparable<Key> {
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

    public Key(Scan scan, Result result) {
      this.namespace = scan.forNamespace().get();
      this.table = scan.forTable().get();
      this.partitionKey = result.getPartitionKey().get();
      this.clusteringKey = result.getClusteringKey();
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
      if (!(o instanceof Key)) {
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
              Comparator.nullsFirst(Comparator.naturalOrder()))
          .result();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("namespace", namespace)
          .add("table", table)
          .add("partitionKey", partitionKey)
          .add("clusteringKey", clusteringKey)
          .toString();
    }
  }

  public static class ReadWriteSets {
    public final String transactionId;
    public final Map<Key, Optional<TransactionResult>> readSetMap;
    public final List<Entry<Key, Put>> writeSet;
    public final List<Entry<Key, Delete>> deleteSet;

    public ReadWriteSets(
        String transactionId,
        Map<Key, Optional<TransactionResult>> readSetMap,
        Collection<Entry<Key, Put>> writeSet,
        Collection<Entry<Key, Delete>> deleteSet) {
      this.transactionId = transactionId;
      this.readSetMap = new HashMap<>(readSetMap);
      this.writeSet = new ArrayList<>(writeSet);
      this.deleteSet = new ArrayList<>(deleteSet);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ReadWriteSets)) return false;
      ReadWriteSets that = (ReadWriteSets) o;
      return Objects.equals(transactionId, that.transactionId)
          && Objects.equals(readSetMap, that.readSetMap)
          && Objects.equals(writeSet, that.writeSet)
          && Objects.equals(deleteSet, that.deleteSet);
    }

    @Override
    public int hashCode() {
      return Objects.hash(transactionId, readSetMap, writeSet, deleteSet);
    }
  }
}
