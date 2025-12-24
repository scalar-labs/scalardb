package com.scalar.db.util;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.GetWithIndex;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.ScanWithIndex;
import com.scalar.db.api.Selection;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.Update;
import com.scalar.db.api.UpdateBuilder;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.UpdateIfExists;
import com.scalar.db.api.Upsert;
import com.scalar.db.api.UpsertBuilder;
import com.scalar.db.common.CoreError;
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public final class ScalarDbUtils {

  private ScalarDbUtils() {}

  @SuppressWarnings("unchecked")
  public static <T extends Operation> List<T> copyAndSetTargetToIfNot(
      List<T> operations, Optional<String> namespace, Optional<String> tableName) {
    return operations.stream()
        .map(o -> (T) copyAndSetTargetToIfNot(o, namespace, tableName))
        .collect(Collectors.toList());
  }

  public static Operation copyAndSetTargetToIfNot(
      Operation operation, Optional<String> namespace, Optional<String> tableName) {
    if (operation instanceof Get) {
      return copyAndSetTargetToIfNot((Get) operation, namespace, tableName);
    } else if (operation instanceof Scan) {
      return copyAndSetTargetToIfNot((Scan) operation, namespace, tableName);
    } else if (operation instanceof Put) {
      return copyAndSetTargetToIfNot((Put) operation, namespace, tableName);
    } else if (operation instanceof Delete) {
      return copyAndSetTargetToIfNot((Delete) operation, namespace, tableName);
    } else if (operation instanceof Insert) {
      return copyAndSetTargetToIfNot((Insert) operation, namespace, tableName);
    } else if (operation instanceof Upsert) {
      return copyAndSetTargetToIfNot((Upsert) operation, namespace, tableName);
    } else {
      assert operation instanceof Update;
      return copyAndSetTargetToIfNot((Update) operation, namespace, tableName);
    }
  }

  public static Get copyAndSetTargetToIfNot(
      Get get, Optional<String> namespace, Optional<String> tableName) {
    GetBuilder.BuildableGetOrGetWithIndexFromExisting builder = Get.newBuilder(get); // copy
    if (!get.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!get.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Get ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  public static Scan copyAndSetTargetToIfNot(
      Scan scan, Optional<String> namespace, Optional<String> tableName) {
    ScanBuilder.BuildableScanOrScanAllFromExisting builder = Scan.newBuilder(scan); // copy
    if (!scan.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!scan.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Scan ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  public static Put copyAndSetTargetToIfNot(
      Put put, Optional<String> namespace, Optional<String> tableName) {
    PutBuilder.BuildableFromExisting builder = Put.newBuilder(put); // copy
    if (!put.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!put.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Put ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  public static Delete copyAndSetTargetToIfNot(
      Delete delete, Optional<String> namespace, Optional<String> tableName) {
    DeleteBuilder.BuildableFromExisting builder = Delete.newBuilder(delete); // copy
    if (!delete.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!delete.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Delete ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  public static Insert copyAndSetTargetToIfNot(
      Insert insert, Optional<String> namespace, Optional<String> tableName) {
    InsertBuilder.BuildableFromExisting builder = Insert.newBuilder(insert); // copy
    if (!insert.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!insert.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Insert ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  public static Upsert copyAndSetTargetToIfNot(
      Upsert upsert, Optional<String> namespace, Optional<String> tableName) {
    UpsertBuilder.BuildableFromExisting builder = Upsert.newBuilder(upsert); // copy
    if (!upsert.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!upsert.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Upsert ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  public static Update copyAndSetTargetToIfNot(
      Update update, Optional<String> namespace, Optional<String> tableName) {
    UpdateBuilder.BuildableFromExisting builder = Update.newBuilder(update); // copy
    if (!update.forNamespace().isPresent() && namespace.isPresent()) {
      builder = builder.namespace(namespace.get());
    }
    if (!update.forTable().isPresent() && tableName.isPresent()) {
      builder = builder.table(tableName.get());
    }
    Update ret = builder.build();
    checkIfTargetIsSet(ret);
    return ret;
  }

  private static void checkIfTargetIsSet(Operation operation) {
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
    List<Column<?>> columns = selection.getPartitionKey().getColumns();
    if (columns.size() == 1) {
      String name = columns.get(0).getName();
      return metadata.getSecondaryIndexNames().contains(name);
    }

    return false;
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
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        throw new UnsupportedOperationException(
            "The type " + column.getDataType() + " is not supported");
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

  public static void checkUpdate(Update update) {
    // check if the condition is UpdateIf
    update
        .getCondition()
        .ifPresent(
            c -> {
              if (!(c instanceof UpdateIf) && !(c instanceof UpdateIfExists)) {
                throw new IllegalArgumentException(
                    CoreError.OPERATION_CHECK_ERROR_UPDATE_CONDITION.buildMessage(update));
              }
            });
  }

  public static Get copyAndPrepareForDynamicFiltering(Get get) {
    GetBuilder.BuildableGetOrGetWithIndexFromExisting builder = Get.newBuilder(get); // copy
    List<String> projections = get.getProjections();
    if (!projections.isEmpty()) {
      // Add columns in conditions into projections to use them in dynamic filtering
      for (String columnName : getColumnNamesUsedIn(get.getConjunctions())) {
        if (!projections.contains(columnName)) {
          builder = builder.projection(columnName);
        }
      }
    }
    return builder.build();
  }

  public static Scan copyAndPrepareForDynamicFiltering(Scan scan) {
    // Ignore limit to control it during dynamic filtering
    ScanBuilder.BuildableScanOrScanAllFromExisting builder = Scan.newBuilder(scan).limit(0); // copy
    List<String> projections = scan.getProjections();
    if (!projections.isEmpty()) {
      // Add columns in conditions into projections to use them in dynamic filtering
      for (String columnName : getColumnNamesUsedIn(scan.getConjunctions())) {
        if (!projections.contains(columnName)) {
          builder = builder.projection(columnName);
        }
      }
    }
    return builder.build();
  }

  private static Set<String> getColumnNamesUsedIn(Set<Conjunction> conjunctions) {
    Set<String> columns = new HashSet<>();
    conjunctions.forEach(
        conjunction ->
            conjunction
                .getConditions()
                .forEach(condition -> columns.add(condition.getColumn().getName())));
    return columns;
  }

  public static boolean columnsMatchAnyOfConjunctions(
      Map<String, Column<?>> columns, Set<Conjunction> conjunctions) {
    for (Conjunction conjunction : conjunctions) {
      boolean allMatched = true;
      for (ConditionalExpression condition : conjunction.getConditions()) {
        if (!columns.containsKey(condition.getColumn().getName())
            || !columnMatchesCondition(columns.get(condition.getColumn().getName()), condition)) {
          allMatched = false;
          break;
        }
      }
      if (allMatched) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static <T> boolean columnMatchesCondition(
      Column<T> column, ConditionalExpression condition) {
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
        // assert condition instanceof LikeExpression;
        return stringMatchesLikeExpression(column.getTextValue(), (LikeExpression) condition);
      default:
        throw new AssertionError("Unknown operator: " + condition.getOperator());
    }
  }

  @VisibleForTesting
  static boolean stringMatchesLikeExpression(String value, LikeExpression likeExpression) {
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
  public static String convertRegexPatternFrom(String likePattern, @Nullable Character escape) {
    Objects.requireNonNull(likePattern, "LIKE pattern must not be null");

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

  public static Key getPartitionKey(Result result, TableMetadata metadata) {
    Optional<Key> key = getKey(result.getColumns(), metadata.getPartitionKeyNames());
    assert key.isPresent();
    return key.get();
  }

  public static Optional<Key> getClusteringKey(Result result, TableMetadata metadata) {
    return getKey(result.getColumns(), metadata.getClusteringKeyNames());
  }

  private static Optional<Key> getKey(Map<String, Column<?>> columns, LinkedHashSet<String> names) {
    if (names.isEmpty()) {
      return Optional.empty();
    }
    Key.Builder builder = Key.newBuilder();
    for (String name : names) {
      Column<?> column = columns.get(name);
      if (column == null) {
        throw new IllegalStateException(CoreError.COLUMN_NOT_FOUND.buildMessage(name));
      }
      builder.add(column);
    }
    return Optional.of(builder.build());
  }
}
