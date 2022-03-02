package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionBuilder.DeleteIfBuilder;
import com.scalar.db.api.ConditionBuilder.PutIfBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.sql.Condition.Operator;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TableNotFoundException;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import com.scalar.db.util.TableMetadataManager;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

// TODO refactoring
public final class SqlUtils {

  private SqlUtils() {}

  public static TableMetadata convertCreateStatementToTableMetadata(
      CreateTableStatement statement) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    statement.columns.forEach((c, d) -> builder.addColumn(c, convertDataType(d)));
    statement.partitionKeyColumnNames.forEach(builder::addPartitionKey);
    statement.clusteringKeyColumnNames.forEach(
        n ->
            builder.addClusteringKey(
                n, convertDataType(statement.clusteringOrders.getOrDefault(n, Order.ASC))));
    statement.secondaryIndexColumnNames.forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  private static com.scalar.db.io.DataType convertDataType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return com.scalar.db.io.DataType.BOOLEAN;
      case INT:
        return com.scalar.db.io.DataType.INT;
      case BIGINT:
        return com.scalar.db.io.DataType.BIGINT;
      case FLOAT:
        return com.scalar.db.io.DataType.FLOAT;
      case DOUBLE:
        return com.scalar.db.io.DataType.DOUBLE;
      case TEXT:
        return com.scalar.db.io.DataType.TEXT;
      case BLOB:
        return com.scalar.db.io.DataType.BLOB;
      default:
        throw new AssertionError();
    }
  }

  private static Scan.Ordering.Order convertDataType(Order order) {
    switch (order) {
      case ASC:
        return Scan.Ordering.Order.ASC;
      case DESC:
        return Scan.Ordering.Order.DESC;
      default:
        throw new AssertionError();
    }
  }

  public static Selection convertSelectStatementToSelection(
      SelectStatement statement, TableMetadata metadata) {
    Key partitionKey =
        createKeyFromConditions(statement.whereConditions, metadata.getPartitionKeyNames());

    ImmutableListMultimap<String, Condition> conditionsMap =
        Multimaps.index(statement.whereConditions, c -> c.columnName);

    if (isGet(conditionsMap, metadata)) {
      Key clusteringKey = null;
      if (!metadata.getClusteringKeyNames().isEmpty()) {
        clusteringKey =
            createKeyFromConditions(statement.whereConditions, metadata.getClusteringKeyNames());
      }
      return new Get(partitionKey, clusteringKey)
          .withProjections(statement.projectedColumnNames)
          .forNamespace(statement.namespaceName)
          .forTable(statement.tableName);
    } else {
      Scan scan =
          new Scan(partitionKey)
              .withProjections(statement.projectedColumnNames)
              .forNamespace(statement.namespaceName)
              .forTable(statement.tableName);
      setClusteringKeyRangeForScan(scan, conditionsMap, metadata);
      if (!statement.orderings.isEmpty()) {
        statement.orderings.forEach(o -> scan.withOrdering(convertOrdering(o)));
      }
      if (statement.limit > 0) {
        scan.withLimit(statement.limit);
      }
      return scan;
    }
  }

  private static boolean isGet(
      ImmutableListMultimap<String, Condition> conditionsMap, TableMetadata metadata) {
    return metadata.getClusteringKeyNames().stream()
        .allMatch(
            n -> {
              if (conditionsMap.get(n).size() == 1) {
                return conditionsMap.get(n).get(0).operator == Operator.IS_EQUAL_TO;
              }
              return false;
            });
  }

  private static void setClusteringKeyRangeForScan(
      Scan scan, ImmutableListMultimap<String, Condition> conditionsMap, TableMetadata metadata) {
    Key.Builder startClusteringKeyBuilder = Key.newBuilder();
    Key.Builder endClusteringKeyBuilder = Key.newBuilder();

    Iterator<String> clusteringKeyNamesIterator = metadata.getClusteringKeyNames().iterator();
    while (clusteringKeyNamesIterator.hasNext()) {
      String clusteringKeyName = clusteringKeyNamesIterator.next();

      ImmutableList<Condition> conditions = conditionsMap.get(clusteringKeyName);
      if (conditions.size() == 1 && conditions.get(0).operator == Operator.IS_EQUAL_TO) {
        addToKeyBuilder(startClusteringKeyBuilder, clusteringKeyName, conditions.get(0).value);
        addToKeyBuilder(endClusteringKeyBuilder, clusteringKeyName, conditions.get(0).value);
        if (!clusteringKeyNamesIterator.hasNext()) {
          scan.withStart(startClusteringKeyBuilder.build(), true);
          scan.withEnd(endClusteringKeyBuilder.build(), true);
        }
      } else if (conditions.isEmpty()) {
        if (startClusteringKeyBuilder.size() > 0) {
          scan.withStart(startClusteringKeyBuilder.build(), true);
        }
        if (endClusteringKeyBuilder.size() > 0) {
          scan.withEnd(endClusteringKeyBuilder.build(), true);
        }
        break;
      } else if (conditions.size() == 1 || conditions.size() == 2) {
        conditions.forEach(
            c -> {
              switch (c.operator) {
                case IS_GREATER_THAN:
                  addToKeyBuilder(startClusteringKeyBuilder, c.columnName, c.value);
                  scan.withStart(startClusteringKeyBuilder.build(), false);
                  break;
                case IS_GREATER_THAN_OR_EQUAL_TO:
                  addToKeyBuilder(startClusteringKeyBuilder, c.columnName, c.value);
                  scan.withStart(startClusteringKeyBuilder.build(), true);
                  break;
                case IS_LESS_THAN:
                  addToKeyBuilder(endClusteringKeyBuilder, c.columnName, c.value);
                  scan.withEnd(endClusteringKeyBuilder.build(), false);
                  break;
                case IS_LESS_THAN_OR_EQUAL_TO:
                  addToKeyBuilder(endClusteringKeyBuilder, c.columnName, c.value);
                  scan.withEnd(endClusteringKeyBuilder.build(), true);
                  break;
                case IS_EQUAL_TO:
                  // TODO
                case IS_NOT_EQUAL_TO:
                  // TODO
                default:
                  throw new AssertionError();
              }
            });
        break;
      } else {
        throw new AssertionError();
      }
    }
  }

  private static Scan.Ordering convertOrdering(Ordering ordering) {
    switch (ordering.order) {
      case ASC:
        return Scan.Ordering.asc(ordering.columnName);
      case DESC:
        return Scan.Ordering.desc(ordering.columnName);
      default:
        throw new AssertionError();
    }
  }

  public static Put convertInsertStatementToPut(InsertStatement statement, TableMetadata metadata) {
    Key partitionKey =
        createKeyFromAssignments(statement.assignments, metadata.getPartitionKeyNames());
    Key clusteringKey = null;
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      clusteringKey =
          createKeyFromAssignments(statement.assignments, metadata.getClusteringKeyNames());
    }
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(statement.namespaceName)
            .forTable(statement.tableName);
    statement.assignments.stream()
        .filter(a -> !metadata.getPartitionKeyNames().contains(a.columnName))
        .filter(a -> !metadata.getClusteringKeyNames().contains(a.columnName))
        .forEach(a -> addValueToPut(put, a.columnName, a.value));
    if (statement.ifNotExists) {
      put.withCondition(ConditionBuilder.putIfNotExists());
    }
    return put;
  }

  public static Put convertUpdateStatementToPut(UpdateStatement statement, TableMetadata metadata) {
    Key partitionKey =
        createKeyFromConditions(statement.whereConditions, metadata.getPartitionKeyNames());
    Key clusteringKey = null;
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      clusteringKey =
          createKeyFromConditions(statement.whereConditions, metadata.getClusteringKeyNames());
    }
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(statement.namespaceName)
            .forTable(statement.tableName);
    statement.assignments.forEach(a -> addValueToPut(put, a.columnName, a.value));
    if (!statement.ifConditions.isEmpty()) {
      put.withCondition(createPutIfFromConditions(statement.ifConditions));
    } else if (statement.ifExists) {
      put.withCondition(ConditionBuilder.putIfExists());
    }
    return put;
  }

  public static Delete convertDeleteStatementToDelete(
      DeleteStatement statement, TableMetadata metadata) {
    Key partitionKey =
        createKeyFromConditions(statement.whereConditions, metadata.getPartitionKeyNames());
    Key clusteringKey = null;
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      clusteringKey =
          createKeyFromConditions(statement.whereConditions, metadata.getClusteringKeyNames());
    }
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(statement.namespaceName)
            .forTable(statement.tableName);
    if (!statement.ifConditions.isEmpty()) {
      delete.withCondition(createDeleteIfFromConditions(statement.ifConditions));
    } else if (statement.ifExists) {
      delete.withCondition(ConditionBuilder.putIfExists());
    }
    return delete;
  }

  private static Key createKeyFromAssignments(
      List<Assignment> assignments, Collection<String> keyColumnNames) {
    Map<String, Assignment> assignmentMap =
        assignments.stream()
            .filter(a -> keyColumnNames.contains(a.columnName))
            .collect(Collectors.toMap(a -> a.columnName, Function.identity()));

    Key.Builder builder = Key.newBuilder();
    keyColumnNames.forEach(n -> addToKeyBuilder(builder, n, assignmentMap.get(n).value));
    return builder.build();
  }

  private static Key createKeyFromConditions(
      List<Condition> conditions, Collection<String> keyColumnNames) {
    Map<String, Condition> conditionsMap =
        conditions.stream()
            .filter(c -> keyColumnNames.contains(c.columnName))
            .collect(Collectors.toMap(a -> a.columnName, Function.identity()));

    Key.Builder builder = Key.newBuilder();
    keyColumnNames.forEach(
        n -> {
          Condition condition = conditionsMap.get(n);
          switch (condition.operator) {
            case IS_EQUAL_TO:
              addToKeyBuilder(builder, n, condition.value);
              break;
            case IS_NOT_EQUAL_TO:
              // TODO
            case IS_GREATER_THAN:
              // TODO
            case IS_GREATER_THAN_OR_EQUAL_TO:
              // TODO
            case IS_LESS_THAN:
              // TODO
            case IS_LESS_THAN_OR_EQUAL_TO:
              // TODO
            default:
              throw new AssertionError();
          }
        });
    return builder.build();
  }

  private static void addToKeyBuilder(Key.Builder builder, String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        builder.addBoolean(columnName, (Boolean) value.value);
        break;
      case INT:
        assert value.value instanceof Integer;
        builder.addInt(columnName, (Integer) value.value);
        break;
      case BIGINT:
        assert value.value instanceof Long;
        builder.addBigInt(columnName, (Long) value.value);
        break;
      case FLOAT:
        assert value.value instanceof Float;
        builder.addFloat(columnName, (Float) value.value);
        break;
      case DOUBLE:
        assert value.value instanceof Double;
        builder.addDouble(columnName, (Double) value.value);
        break;
      case TEXT:
        assert value.value instanceof String;
        builder.addText(columnName, (String) value.value);
        break;
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        builder.addBlob(columnName, (ByteBuffer) value.value);
        break;
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        builder.addBlob(columnName, (byte[]) value.value);
        break;
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  private static void addValueToPut(Put put, String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        put.withBooleanValue(columnName, (Boolean) value.value);
        break;
      case INT:
        assert value.value instanceof Integer;
        put.withIntValue(columnName, (Integer) value.value);
        break;
      case BIGINT:
        assert value.value instanceof Long;
        put.withBigIntValue(columnName, (Long) value.value);
        break;
      case FLOAT:
        assert value.value instanceof Float;
        put.withFloatValue(columnName, (Float) value.value);
        break;
      case DOUBLE:
        assert value.value instanceof Double;
        put.withDoubleValue(columnName, (Double) value.value);
        break;
      case TEXT:
        assert value.value instanceof String;
        put.withTextValue(columnName, (String) value.value);
        break;
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        put.withBlobValue(columnName, (ByteBuffer) value.value);
        break;
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        put.withBlobValue(columnName, (byte[]) value.value);
        break;
      case NULL:
        put.withNullValue(columnName);
        break;
      default:
        throw new AssertionError();
    }
  }

  private static PutIf createPutIfFromConditions(List<Condition> conditions) {
    List<ConditionalExpression> conditionalExpressions = createConditionalExpressions(conditions);
    PutIfBuilder putIfBuilder = ConditionBuilder.putIf(conditionalExpressions.get(0));
    for (int i = 1; i < conditionalExpressions.size(); i++) {
      putIfBuilder.and(conditionalExpressions.get(i));
    }
    return putIfBuilder.build();
  }

  private static DeleteIf createDeleteIfFromConditions(List<Condition> conditions) {
    List<ConditionalExpression> conditionalExpressions = createConditionalExpressions(conditions);
    DeleteIfBuilder deleteIfBuilder = ConditionBuilder.deleteIf(conditionalExpressions.get(0));
    for (int i = 1; i < conditionalExpressions.size(); i++) {
      deleteIfBuilder.and(conditionalExpressions.get(i));
    }
    return deleteIfBuilder.build();
  }

  private static List<ConditionalExpression> createConditionalExpressions(
      List<Condition> conditions) {
    return conditions.stream()
        .map(
            c -> {
              switch (c.operator) {
                case IS_EQUAL_TO:
                  return createIsEqualToConditionalExpression(c.columnName, c.value);
                case IS_NOT_EQUAL_TO:
                  return createIsNotEqualToConditionalExpression(c.columnName, c.value);
                case IS_GREATER_THAN:
                  return createIsGreaterThanConditionalExpression(c.columnName, c.value);
                case IS_GREATER_THAN_OR_EQUAL_TO:
                  return createIsGreaterThanOrEqualToConditionalExpression(c.columnName, c.value);
                case IS_LESS_THAN:
                  return createIsLessThanConditionalExpression(c.columnName, c.value);
                case IS_LESS_THAN_OR_EQUAL_TO:
                  return createIsLessThanOrEqualToConditionalExpression(c.columnName, c.value);
                default:
                  throw new AssertionError();
              }
            })
        .collect(Collectors.toList());
  }

  private static ConditionalExpression createIsEqualToConditionalExpression(
      String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        return ConditionBuilder.column(columnName).isEqualToBoolean((Boolean) value.value);
      case INT:
        assert value.value instanceof Integer;
        return ConditionBuilder.column(columnName).isEqualToInt((Integer) value.value);
      case BIGINT:
        assert value.value instanceof Long;
        return ConditionBuilder.column(columnName).isEqualToBigInt((Long) value.value);
      case FLOAT:
        assert value.value instanceof Float;
        return ConditionBuilder.column(columnName).isEqualToFloat((Float) value.value);
      case DOUBLE:
        assert value.value instanceof Double;
        return ConditionBuilder.column(columnName).isEqualToDouble((Double) value.value);
      case TEXT:
        assert value.value instanceof String;
        return ConditionBuilder.column(columnName).isEqualToText((String) value.value);
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        return ConditionBuilder.column(columnName).isEqualToBlob((ByteBuffer) value.value);
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        return ConditionBuilder.column(columnName).isEqualToBlob((byte[]) value.value);
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  private static ConditionalExpression createIsNotEqualToConditionalExpression(
      String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        return ConditionBuilder.column(columnName).isNotEqualToBoolean((Boolean) value.value);
      case INT:
        assert value.value instanceof Integer;
        return ConditionBuilder.column(columnName).isNotEqualToInt((Integer) value.value);
      case BIGINT:
        assert value.value instanceof Long;
        return ConditionBuilder.column(columnName).isNotEqualToBigInt((Long) value.value);
      case FLOAT:
        assert value.value instanceof Float;
        return ConditionBuilder.column(columnName).isNotEqualToFloat((Float) value.value);
      case DOUBLE:
        assert value.value instanceof Double;
        return ConditionBuilder.column(columnName).isNotEqualToDouble((Double) value.value);
      case TEXT:
        assert value.value instanceof String;
        return ConditionBuilder.column(columnName).isNotEqualToText((String) value.value);
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        return ConditionBuilder.column(columnName).isNotEqualToBlob((ByteBuffer) value.value);
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        return ConditionBuilder.column(columnName).isNotEqualToBlob((byte[]) value.value);
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  private static ConditionalExpression createIsGreaterThanConditionalExpression(
      String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        return ConditionBuilder.column(columnName).isGreaterThanBoolean((Boolean) value.value);
      case INT:
        assert value.value instanceof Integer;
        return ConditionBuilder.column(columnName).isGreaterThanInt((Integer) value.value);
      case BIGINT:
        assert value.value instanceof Long;
        return ConditionBuilder.column(columnName).isGreaterThanBigInt((Long) value.value);
      case FLOAT:
        assert value.value instanceof Float;
        return ConditionBuilder.column(columnName).isGreaterThanFloat((Float) value.value);
      case DOUBLE:
        assert value.value instanceof Double;
        return ConditionBuilder.column(columnName).isGreaterThanDouble((Double) value.value);
      case TEXT:
        assert value.value instanceof String;
        return ConditionBuilder.column(columnName).isGreaterThanText((String) value.value);
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        return ConditionBuilder.column(columnName).isGreaterThanBlob((ByteBuffer) value.value);
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        return ConditionBuilder.column(columnName).isGreaterThanBlob((byte[]) value.value);
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  private static ConditionalExpression createIsGreaterThanOrEqualToConditionalExpression(
      String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        return ConditionBuilder.column(columnName)
            .isGreaterThanOrEqualToBoolean((Boolean) value.value);
      case INT:
        assert value.value instanceof Integer;
        return ConditionBuilder.column(columnName).isGreaterThanOrEqualToInt((Integer) value.value);
      case BIGINT:
        assert value.value instanceof Long;
        return ConditionBuilder.column(columnName).isGreaterThanOrEqualToBigInt((Long) value.value);
      case FLOAT:
        assert value.value instanceof Float;
        return ConditionBuilder.column(columnName).isGreaterThanOrEqualToFloat((Float) value.value);
      case DOUBLE:
        assert value.value instanceof Double;
        return ConditionBuilder.column(columnName)
            .isGreaterThanOrEqualToDouble((Double) value.value);
      case TEXT:
        assert value.value instanceof String;
        return ConditionBuilder.column(columnName).isGreaterThanOrEqualToText((String) value.value);
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        return ConditionBuilder.column(columnName)
            .isGreaterThanOrEqualToBlob((ByteBuffer) value.value);
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        return ConditionBuilder.column(columnName).isGreaterThanOrEqualToBlob((byte[]) value.value);
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  private static ConditionalExpression createIsLessThanConditionalExpression(
      String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        return ConditionBuilder.column(columnName).isLessThanBoolean((Boolean) value.value);
      case INT:
        assert value.value instanceof Integer;
        return ConditionBuilder.column(columnName).isLessThanInt((Integer) value.value);
      case BIGINT:
        assert value.value instanceof Long;
        return ConditionBuilder.column(columnName).isLessThanBigInt((Long) value.value);
      case FLOAT:
        assert value.value instanceof Float;
        return ConditionBuilder.column(columnName).isLessThanFloat((Float) value.value);
      case DOUBLE:
        assert value.value instanceof Double;
        return ConditionBuilder.column(columnName).isLessThanDouble((Double) value.value);
      case TEXT:
        assert value.value instanceof String;
        return ConditionBuilder.column(columnName).isLessThanText((String) value.value);
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        return ConditionBuilder.column(columnName).isLessThanBlob((ByteBuffer) value.value);
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        return ConditionBuilder.column(columnName).isLessThanBlob((byte[]) value.value);
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  private static ConditionalExpression createIsLessThanOrEqualToConditionalExpression(
      String columnName, Value value) {
    switch (value.type) {
      case BOOLEAN:
        assert value.value instanceof Boolean;
        return ConditionBuilder.column(columnName)
            .isLessThanOrEqualToBoolean((Boolean) value.value);
      case INT:
        assert value.value instanceof Integer;
        return ConditionBuilder.column(columnName).isLessThanOrEqualToInt((Integer) value.value);
      case BIGINT:
        assert value.value instanceof Long;
        return ConditionBuilder.column(columnName).isLessThanOrEqualToBigInt((Long) value.value);
      case FLOAT:
        assert value.value instanceof Float;
        return ConditionBuilder.column(columnName).isLessThanOrEqualToFloat((Float) value.value);
      case DOUBLE:
        assert value.value instanceof Double;
        return ConditionBuilder.column(columnName).isLessThanOrEqualToDouble((Double) value.value);
      case TEXT:
        assert value.value instanceof String;
        return ConditionBuilder.column(columnName).isLessThanOrEqualToText((String) value.value);
      case BLOB_BYTE_BUFFER:
        assert value.value instanceof ByteBuffer;
        return ConditionBuilder.column(columnName)
            .isLessThanOrEqualToBlob((ByteBuffer) value.value);
      case BLOB_BYTES:
        assert value.value instanceof byte[];
        return ConditionBuilder.column(columnName).isLessThanOrEqualToBlob((byte[]) value.value);
      case NULL:
        // TODO
      default:
        throw new AssertionError();
    }
  }

  public static TableMetadata getTableMetadata(
      TableMetadataManager tableMetadataManager, String namespaceName, String tableName) {
    try {
      TableMetadata metadata = tableMetadataManager.getTableMetadata(namespaceName, tableName);
      if (metadata == null) {
        throw new TableNotFoundException(namespaceName, tableName);
      }
      return metadata;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get a table metadata", e);
    }
  }
}
