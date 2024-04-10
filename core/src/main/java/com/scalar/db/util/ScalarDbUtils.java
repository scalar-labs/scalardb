package com.scalar.db.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetWithIndex;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.ScanWithIndex;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.PrimaryKey;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public final class ScalarDbUtils {

  private ScalarDbUtils() {}

  @SuppressWarnings("unchecked")
  public static <T extends Mutation> List<T> copyAndSetTargetToIfNot(
      List<T> mutations, Optional<String> namespace, Optional<String> tableName) {
    return mutations.stream()
        .map(
            m -> {
              if (m instanceof Put) {
                return (T) copyAndSetTargetToIfNot((Put) m, namespace, tableName);
              } else { // Delete
                return (T) copyAndSetTargetToIfNot((Delete) m, namespace, tableName);
              }
            })
        .collect(Collectors.toList());
  }

  public static Get copyAndSetTargetToIfNot(
      Get get, Optional<String> namespace, Optional<String> tableName) {
    Get ret = new Get(get); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  public static Scan copyAndSetTargetToIfNot(
      Scan scan, Optional<String> namespace, Optional<String> tableName) {
    Scan ret;
    if (scan instanceof ScanAll) {
      ret = new ScanAll((ScanAll) scan); // copy
    } else if (scan instanceof ScanWithIndex) {
      ret = new ScanWithIndex((ScanWithIndex) scan); // copy
    } else {
      ret = new Scan(scan); // copy
    }
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  public static Mutation copyAndSetTargetToIfNot(
      Mutation mutation, Optional<String> namespace, Optional<String> tableName) {
    if (mutation instanceof Put) {
      return copyAndSetTargetToIfNot((Put) mutation, namespace, tableName);
    } else {
      assert mutation instanceof Delete;
      return copyAndSetTargetToIfNot((Delete) mutation, namespace, tableName);
    }
  }

  public static Put copyAndSetTargetToIfNot(
      Put put, Optional<String> namespace, Optional<String> tableName) {
    Put ret = new Put(put); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  public static Delete copyAndSetTargetToIfNot(
      Delete delete, Optional<String> namespace, Optional<String> tableName) {
    Delete ret = new Delete(delete); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  private static void setTargetToIfNot(
      Operation operation, Optional<String> namespace, Optional<String> tableName) {
    if (!operation.forNamespace().isPresent()) {
      operation.forNamespace(namespace.orElse(null));
    }
    if (!operation.forTable().isPresent()) {
      operation.forTable(tableName.orElse(null));
    }
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_DOES_NOT_HAVE_TARGET_NAMESPACE_OR_TABLE_NAME.buildMessage(operation));
    }
  }

  public static boolean isSecondaryIndexSpecified(Selection selection, TableMetadata metadata) {
    if (selection instanceof GetWithIndex || selection instanceof ScanWithIndex) {
      return true;
    }

    // We need to keep this for backward compatibility. We will remove it in release 5.0.0.
    List<Value<?>> keyValues = selection.getPartitionKey().get();
    if (keyValues.size() == 1) {
      String name = keyValues.get(0).getName();
      return metadata.getSecondaryIndexNames().contains(name);
    }

    return false;
  }

  public static void addProjectionsForKeys(Selection selection, TableMetadata metadata) {
    if (selection.getProjections().isEmpty()) { // meaning projecting all
      return;
    }
    Streams.concat(
            metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
        .filter(n -> !selection.getProjections().contains(n))
        .forEach(selection::withProjection);
  }

  public static <E> E pollUninterruptibly(BlockingQueue<E> queue, long timeout, TimeUnit unit) {
    boolean interrupted = false;
    try {
      long remainingNanos = unit.toNanos(timeout);
      long end = System.nanoTime() + remainingNanos;

      while (true) {
        try {
          return queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          interrupted = true;
          remainingNanos = end - System.nanoTime();
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static <T> Future<T> takeUninterruptibly(CompletionService<T> completionService) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return completionService.take();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Return a fully qualified table name
   *
   * @param namespace a namespace
   * @param table a table
   * @return a fully qualified table name
   */
  public static String getFullTableName(String namespace, String table) {
    return namespace + "." + table;
  }

  /**
   * Converts {@code Column} to {@code Value}.
   *
   * @param column a column to convert
   * @return a value converted from the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public static Value<?> toValue(Column<?> column) {
    switch (column.getDataType()) {
      case BOOLEAN:
        return new BooleanValue(column.getName(), column.getBooleanValue());
      case INT:
        return new IntValue(column.getName(), column.getIntValue());
      case BIGINT:
        return new BigIntValue(column.getName(), column.getBigIntValue());
      case FLOAT:
        return new FloatValue(column.getName(), column.getFloatValue());
      case DOUBLE:
        return new DoubleValue(column.getName(), column.getDoubleValue());
      case TEXT:
        return new TextValue(column.getName(), column.getTextValue());
      case BLOB:
        return new BlobValue(column.getName(), column.getBlobValue());
      default:
        throw new AssertionError();
    }
  }

  /**
   * Converts {@code Value} to {@code Column}.
   *
   * @param value a value to convert
   * @return a column converted from the value
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public static Column<?> toColumn(Value<?> value) {
    switch (value.getDataType()) {
      case BOOLEAN:
        return BooleanColumn.of(value.getName(), value.getAsBoolean());
      case INT:
        return IntColumn.of(value.getName(), value.getAsInt());
      case BIGINT:
        return BigIntColumn.of(value.getName(), value.getAsLong());
      case FLOAT:
        return FloatColumn.of(value.getName(), value.getAsFloat());
      case DOUBLE:
        return DoubleColumn.of(value.getName(), value.getAsDouble());
      case TEXT:
        return TextColumn.of(value.getName(), value.getAsString().orElse(null));
      case BLOB:
        return BlobColumn.of(value.getName(), value.getAsBytes().orElse(null));
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns true if the mutation set is overlapped with the scan range.
   *
   * @param scan a scan operation to check
   * @param scannedKeys a list of primary keys that have been scanned with the scan operation
   * @param mutationSet a set of mutations to check
   * @return true if the mutation set is overlapped with the scan range
   */
  public static boolean isMutationSetOverlappedWith(
      Scan scan, List<PrimaryKey> scannedKeys, Map<PrimaryKey, ? extends Mutation> mutationSet) {
    if (scan instanceof ScanAll) {
      return isMutationSetOverlappedWith((ScanAll) scan, scannedKeys, mutationSet);
    }

    for (Map.Entry<PrimaryKey, ? extends Mutation> entry : mutationSet.entrySet()) {
      Mutation mutation = entry.getValue();

      if (!mutation.forNamespace().equals(scan.forNamespace())
          || !mutation.forTable().equals(scan.forTable())
          || !mutation.getPartitionKey().equals(scan.getPartitionKey())) {
        continue;
      }

      // If partition keys match and a primary key does not have a clustering key
      if (!mutation.getClusteringKey().isPresent()) {
        return true;
      }

      Key writtenKey = mutation.getClusteringKey().get();
      boolean isStartGiven = scan.getStartClusteringKey().isPresent();
      boolean isEndGiven = scan.getEndClusteringKey().isPresent();

      // If no range is specified, which means it scans the whole partition space
      if (!isStartGiven && !isEndGiven) {
        return true;
      }

      if (isStartGiven && isEndGiven) {
        Key startKey = scan.getStartClusteringKey().get();
        Key endKey = scan.getEndClusteringKey().get();
        // If startKey <= writtenKey <= endKey
        if ((scan.getStartInclusive() && writtenKey.equals(startKey))
            || (writtenKey.compareTo(startKey) > 0 && writtenKey.compareTo(endKey) < 0)
            || (scan.getEndInclusive() && writtenKey.equals(endKey))) {
          return true;
        }
      }

      if (isStartGiven && !isEndGiven) {
        Key startKey = scan.getStartClusteringKey().get();
        // If startKey <= writtenKey
        if ((scan.getStartInclusive() && startKey.equals(writtenKey))
            || writtenKey.compareTo(startKey) > 0) {
          return true;
        }
      }

      if (!isStartGiven) {
        Key endKey = scan.getEndClusteringKey().get();
        // If writtenKey <= endKey
        if ((scan.getEndInclusive() && writtenKey.equals(endKey))
            || writtenKey.compareTo(endKey) < 0) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isMutationSetOverlappedWith(
      ScanAll scan, List<PrimaryKey> scannedKeys, Map<PrimaryKey, ? extends Mutation> mutationSet) {
    for (Map.Entry<PrimaryKey, ? extends Mutation> entry : mutationSet.entrySet()) {
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
      if (scannedKeys.contains(entry.getKey())) {
        return true;
      }

      // Check for case 3
      Mutation mutation = entry.getValue();
      if (!mutation.forNamespace().equals(scan.forNamespace())
          || !mutation.forTable().equals(scan.forTable())) {
        continue;
      }

      if (scan.getConjunctions().isEmpty()) {
        return true;
      }

      if (!(mutation instanceof Put)) {
        continue;
      }

      Put put = (Put) mutation;

      Map<String, Column<?>> columns = new HashMap<>(put.getColumns());
      put.getPartitionKey().getColumns().forEach(column -> columns.put(column.getName(), column));
      put.getClusteringKey()
          .ifPresent(
              key -> key.getColumns().forEach(column -> columns.put(column.getName(), column)));
      for (Scan.Conjunction conjunction : scan.getConjunctions()) {
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
  private static <T> boolean match(Column<T> column, ConditionalExpression condition) {
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
        throw new AssertionError("Unknown operator: " + condition.getOperator());
    }
  }

  @VisibleForTesting
  static boolean isMatched(LikeExpression likeExpression, String value) {
    String escape = likeExpression.getEscape();
    String regexPattern =
        convertRegexPatternFrom(
            likeExpression.getTextValue(), escape.isEmpty() ? null : escape.charAt(0));
    if (likeExpression.getOperator().equals(ConditionalExpression.Operator.LIKE)) {
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
  private static String convertRegexPatternFrom(String likePattern, @Nullable Character escape) {
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
}
