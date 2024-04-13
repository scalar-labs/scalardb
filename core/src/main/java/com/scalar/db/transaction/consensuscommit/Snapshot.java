package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.util.ScalarDbUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
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
  private final ConcurrentMap<Key, Optional<TransactionResult>> readSet;
  private final Map<Scan, List<Key>> scanSet;
  private final Map<Key, Put> writeSet;
  private final Map<Key, Delete> deleteSet;

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
      ConcurrentMap<Key, Optional<TransactionResult>> readSet,
      Map<Scan, List<Key>> scanSet,
      Map<Key, Put> writeSet,
      Map<Key, Delete> deleteSet) {
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
  public void put(Key key, Optional<TransactionResult> result) {
    readSet.put(key, result);
  }

  public void put(Scan scan, List<Key> keys) {
    scanSet.put(scan, keys);
  }

  public void put(Key key, Put put) {
    if (deleteSet.containsKey(key)) {
      throw new IllegalArgumentException("Writing already deleted data is not allowed");
    }
    if (writeSet.containsKey(key)) {
      // merge the previous put in the write set and the new put
      Put originalPut = writeSet.get(key);
      put.getColumns().values().forEach(originalPut::withValue);
    } else {
      writeSet.put(key, put);
    }
  }

  public void put(Key key, Delete delete) {
    writeSet.remove(key);
    deleteSet.put(key, delete);
  }

  public boolean containsKeyInReadSet(Key key) {
    return readSet.containsKey(key);
  }

  public Optional<TransactionResult> getFromReadSet(Key key) {
    return readSet.getOrDefault(key, Optional.empty());
  }

  public List<Put> getPutsInWriteSet() {
    return new ArrayList<>(writeSet.values());
  }

  public List<Delete> getDeletesInDeleteSet() {
    return new ArrayList<>(deleteSet.values());
  }

  public Optional<TransactionResult> get(Key key) throws CrudException {
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
        "Getting data neither in the read set nor the delete set is not allowed");
  }

  private TableMetadata getTableMetadata(Key key) throws CrudException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(key.getNamespace(), key.getTable());
      if (metadata == null) {
        throw new IllegalArgumentException(
            "The specified table is not found: "
                + ScalarDbUtils.getFullTableName(key.getNamespace(), key.getTable()));
      }
      return metadata.getTableMetadata();
    } catch (ExecutionException e) {
      throw new CrudException("Getting a table metadata failed", e, id);
    }
  }

  private TableMetadata getTableMetadata(Scan scan) throws ExecutionException {
    try {
      TransactionTableMetadata metadata =
          tableMetadataManager.getTransactionTableMetadata(
              scan.forNamespace().get(), scan.forTable().get());
      if (metadata == null) {
        throw new IllegalArgumentException(
            "The specified table is not found: "
                + ScalarDbUtils.getFullTableName(scan.forNamespace().get(), scan.forTable().get()));
      }
      return metadata.getTableMetadata();
    } catch (ExecutionException e) {
      throw new ExecutionException("Getting a table metadata failed", e);
    }
  }

  public Optional<List<Key>> get(Scan scan) {
    if (scanSet.containsKey(scan)) {
      return Optional.ofNullable(scanSet.get(scan));
    }
    return Optional.empty();
  }

  public void verify(Scan scan) {
    if (isWriteSetOverlappedWith(scan)) {
      throw new IllegalArgumentException("Reading already written data is not allowed");
    }
  }

  public void to(MutationComposer composer)
      throws ExecutionException, PreparationConflictException {
    toSerializableWithExtraWrite(composer);

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

  private boolean isWriteSetOverlappedWith(Scan scan) {
    if (scan instanceof ScanAll) {
      return isWriteSetOverlappedWith((ScanAll) scan);
    }

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

  private boolean isWriteSetOverlappedWith(ScanAll scan) {
    for (Map.Entry<Key, Put> entry : writeSet.entrySet()) {
      // We need to consider three cases here to prevent scan-after-write.
      //   1) A put operation overlaps the scan range regardless of the update (put) results.
      //   2) A put operation does not overlap the scan range as a result of the update.
      //   3) A put operation overlaps the scan range as a result of the update.
      // See the following examples. Assume that we have a table with two columns whose names are
      // "key" and "value" and two records in the table: (key=1, value=2) and (key=2, key=3).
      // Case 2 covers a transaction that puts (1, 4) and then scans "where value < 3". In this
      // case, there is no overlap, but we intentionally prohibit it due to the consistency and
      // simplicity of snapshot management. We can find case 2 using the scan results.
      // Case 3 covers a transaction that puts (2, 2) and then scans "where value < 3". In this
      // case, we cannot find the overlap using the scan results since the database is not updated
      // yet. Thus, we need to evaluate if the scan condition potentially matches put operations.

      // Check for cases 1 and 2
      if (scanSet.get(scan).contains(entry.getKey())) {
        return true;
      }

      // Check for case 3
      Put put = entry.getValue();
      if (!put.forNamespace().equals(scan.forNamespace())
          || !put.forTable().equals(scan.forTable())) {
        continue;
      }

      if (scan.getConjunctions().isEmpty()) {
        return true;
      }

      Map<String, Column<?>> columns = new HashMap<>(put.getColumns());
      put.getPartitionKey().getColumns().forEach(column -> columns.put(column.getName(), column));
      put.getClusteringKey()
          .ifPresent(
              key -> key.getColumns().forEach(column -> columns.put(column.getName(), column)));
      for (Conjunction conjunction : scan.getConjunctions()) {
        boolean allMatched = true;
        for (ConditionalExpression condition : conjunction.getConditions()) {
          if (!columns.containsKey(condition.getColumn().getName())
              || !match(columns.get(condition.getColumn().getName()), condition)) {
            allMatched = false;
            break;
          }
        }
        if (allMatched) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private <T> boolean match(Column<T> column, ConditionalExpression condition) {
    assert column.getClass() == condition.getColumn().getClass();
    switch (condition.getOperator()) {
      case EQ:
      case IS_NULL:
        return column.equals(condition.getColumn());
      case NE:
      case IS_NOT_NULL:
        return !column.equals(condition.getColumn());
      case GT:
        return column.compareTo((Column<T>) condition.getColumn()) > 0;
      case GTE:
        return column.compareTo((Column<T>) condition.getColumn()) >= 0;
      case LT:
        return column.compareTo((Column<T>) condition.getColumn()) < 0;
      case LTE:
        return column.compareTo((Column<T>) condition.getColumn()) <= 0;
      case LIKE:
      case NOT_LIKE:
        return isMatched((LikeExpression) condition, column.getTextValue());
      default:
        throw new IllegalArgumentException("Unknown operator: " + condition.getOperator());
    }
  }

  @VisibleForTesting
  void toSerializableWithExtraWrite(MutationComposer composer)
      throws ExecutionException, PreparationConflictException {
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

  @VisibleForTesting
  void toSerializableWithExtraRead(DistributedStorage storage)
      throws ExecutionException, ValidationConflictException {
    if (!isExtraReadEnabled()) {
      return;
    }

    List<ParallelExecutorTask> tasks = new ArrayList<>();

    // Read set by scan is re-validated to check if there is no anti-dependency
    for (Map.Entry<Scan, List<Key>> entry : scanSet.entrySet()) {
      tasks.add(
          () -> {
            Map<Key, TransactionResult> currentReadMap = new HashMap<>();
            Set<Key> validatedReadSet = new HashSet<>();
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
                currentReadMap.put(new Key(scan, result), transactionResult);
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

            for (Key key : entry.getValue()) {
              if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
                continue;
              }
              // Check if read records are not changed
              TransactionResult latestResult = currentReadMap.get(key);
              if (isChanged(Optional.ofNullable(latestResult), readSet.get(key))) {
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
            // only get tx_id and tx_version columns because we use only them to compare
            Get get =
                new Get(key.getPartitionKey(), key.getClusteringKey().orElse(null))
                    .withProjection(Attribute.ID)
                    .withProjection(Attribute.VERSION)
                    .withConsistency(Consistency.LINEARIZABLE)
                    .forNamespace(key.getNamespace())
                    .forTable(key.getTable());

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
        "Reading empty records might cause write skew anomaly so aborting the transaction for safety",
        id);
  }

  private void throwExceptionDueToAntiDependency() throws ValidationConflictException {
    throw new ValidationConflictException("Anti-dependency found. Aborting the transaction", id);
  }

  private boolean isExtraReadEnabled() {
    return isolation == Isolation.SERIALIZABLE && strategy == SerializableStrategy.EXTRA_READ;
  }

  public boolean isValidationRequired() {
    return isExtraReadEnabled();
  }

  @VisibleForTesting
  boolean isMatched(LikeExpression likeExpression, String value) {
    String escape = likeExpression.getEscape();
    String regexPattern =
        convertRegexPatternFrom(
            likeExpression.getTextValue(), escape.isEmpty() ? null : escape.charAt(0));
    if (likeExpression.getOperator().equals(Operator.LIKE)) {
      return value != null && Pattern.compile(regexPattern).matcher(value).matches();
    } else {
      return value != null && !Pattern.compile(regexPattern).matcher(value).matches();
    }
  }

  /**
   * Convert SQL 'like' pattern to a Java regular expression. Underscores (_) are converted to '.'
   * and percent signs (%) are converted to '.*', other characters are quoted literally. If an
   * escape character specified, escaping is done for '_', '%', and the escape character itself.
   * Although we validate the pattern when constructing {@code LikeExpression}, we will assert it
   * just in case. This method is implemented referencing the following Spark SQL implementation.
   * https://github.com/apache/spark/blob/a8eadebd686caa110c4077f4199d11e797146dc5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/StringUtils.scala
   *
   * @param likePattern a SQL LIKE pattern to convert
   * @param escape an escape character.
   * @return the equivalent Java regular expression of the given pattern
   */
  private String convertRegexPatternFrom(String likePattern, @Nullable Character escape) {
    assert likePattern != null : "LIKE pattern must not be null";

    StringBuilder out = new StringBuilder();
    char[] chars = likePattern.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      if (escape != null && c == escape && i + 1 < chars.length) {
        char nextChar = chars[++i];
        if (nextChar == '_' || nextChar == '%') {
          out.append(Pattern.quote(Character.toString(nextChar)));
        } else if (nextChar == escape) {
          out.append(Pattern.quote(Character.toString(nextChar)));
        } else {
          throw new AssertionError("LIKE pattern must not include only escape character");
        }
      } else if (escape != null && c == escape) {
        throw new AssertionError("LIKE pattern must not end with escape character");
      } else if (c == '_') {
        out.append(".");
      } else if (c == '%') {
        out.append(".*");
      } else {
        out.append(Pattern.quote(Character.toString(c)));
      }
    }

    return "(?s)" + out; // (?s) enables dotall mode, causing "." to match new lines
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
}
