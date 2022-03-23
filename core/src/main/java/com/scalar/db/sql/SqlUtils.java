package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.sql.Predicate.Operator;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TableNotFoundException;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SqlUtils {

  private SqlUtils() {}

  public static Selection convertSelectStatementToSelection(
      SelectStatement statement, com.scalar.db.api.TableMetadata metadata) {
    Key partitionKey =
        createKeyFromPredicates(statement.predicates, metadata.getPartitionKeyNames());

    ImmutableListMultimap<String, Predicate> predicatesMap =
        Multimaps.index(statement.predicates, c -> c.columnName);

    if (isGet(predicatesMap, metadata)) {
      Key clusteringKey = null;
      if (!metadata.getClusteringKeyNames().isEmpty()) {
        clusteringKey =
            createKeyFromPredicates(statement.predicates, metadata.getClusteringKeyNames());
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
      setClusteringKeyRangeForScan(scan, predicatesMap, metadata);
      if (!statement.clusteringOrderings.isEmpty()) {
        statement.clusteringOrderings.forEach(o -> scan.withOrdering(convertOrdering(o)));
      }
      if (statement.limit > 0) {
        scan.withLimit(statement.limit);
      }
      return scan;
    }
  }

  private static boolean isGet(
      ImmutableListMultimap<String, Predicate> predicatesMap,
      com.scalar.db.api.TableMetadata metadata) {
    return metadata.getClusteringKeyNames().stream()
        .allMatch(
            n ->
                predicatesMap.containsKey(n)
                    && predicatesMap.get(n).size() == 1
                    && predicatesMap.get(n).get(0).operator == Operator.EQUAL_TO);
  }

  private static void setClusteringKeyRangeForScan(
      Scan scan,
      ImmutableListMultimap<String, Predicate> predicatesMap,
      com.scalar.db.api.TableMetadata metadata) {
    Key.Builder startClusteringKeyBuilder = Key.newBuilder();
    Key.Builder endClusteringKeyBuilder = Key.newBuilder();

    Iterator<String> clusteringKeyNamesIterator = metadata.getClusteringKeyNames().iterator();
    while (clusteringKeyNamesIterator.hasNext()) {
      String clusteringKeyName = clusteringKeyNamesIterator.next();

      ImmutableList<Predicate> predicates = predicatesMap.get(clusteringKeyName);
      if (predicates.size() == 1 && predicates.get(0).operator == Operator.EQUAL_TO) {
        addToKeyBuilder(startClusteringKeyBuilder, clusteringKeyName, predicates.get(0).value);
        addToKeyBuilder(endClusteringKeyBuilder, clusteringKeyName, predicates.get(0).value);
        if (!clusteringKeyNamesIterator.hasNext()) {
          scan.withStart(startClusteringKeyBuilder.build(), true);
          scan.withEnd(endClusteringKeyBuilder.build(), true);
        }
      } else if (predicates.isEmpty()) {
        if (startClusteringKeyBuilder.size() > 0) {
          scan.withStart(startClusteringKeyBuilder.build(), true);
        }
        if (endClusteringKeyBuilder.size() > 0) {
          scan.withEnd(endClusteringKeyBuilder.build(), true);
        }
        break;
      } else if (predicates.size() == 1 || predicates.size() == 2) {
        predicates.forEach(
            c -> {
              switch (c.operator) {
                case GREATER_THAN:
                  addToKeyBuilder(startClusteringKeyBuilder, c.columnName, c.value);
                  scan.withStart(startClusteringKeyBuilder.build(), false);
                  break;
                case GREATER_THAN_OR_EQUAL_TO:
                  addToKeyBuilder(startClusteringKeyBuilder, c.columnName, c.value);
                  scan.withStart(startClusteringKeyBuilder.build(), true);
                  break;
                case LESS_THAN:
                  addToKeyBuilder(endClusteringKeyBuilder, c.columnName, c.value);
                  scan.withEnd(endClusteringKeyBuilder.build(), false);
                  break;
                case LESS_THAN_OR_EQUAL_TO:
                  addToKeyBuilder(endClusteringKeyBuilder, c.columnName, c.value);
                  scan.withEnd(endClusteringKeyBuilder.build(), true);
                  break;
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

  private static Scan.Ordering convertOrdering(ClusteringOrdering clusteringOrdering) {
    switch (clusteringOrdering.clusteringOrder) {
      case ASC:
        return Scan.Ordering.asc(clusteringOrdering.columnName);
      case DESC:
        return Scan.Ordering.desc(clusteringOrdering.columnName);
      default:
        throw new AssertionError();
    }
  }

  public static Put convertInsertStatementToPut(
      InsertStatement statement, com.scalar.db.api.TableMetadata metadata) {
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
        .forEach(a -> addValueToPut(put, a.columnName, a.value, metadata));
    return put;
  }

  public static Put convertUpdateStatementToPut(
      UpdateStatement statement, com.scalar.db.api.TableMetadata metadata) {
    Key partitionKey =
        createKeyFromPredicates(statement.predicates, metadata.getPartitionKeyNames());
    Key clusteringKey = null;
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      clusteringKey =
          createKeyFromPredicates(statement.predicates, metadata.getClusteringKeyNames());
    }
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(statement.namespaceName)
            .forTable(statement.tableName);
    statement.assignments.forEach(a -> addValueToPut(put, a.columnName, a.value, metadata));
    return put;
  }

  public static Delete convertDeleteStatementToDelete(
      DeleteStatement statement, com.scalar.db.api.TableMetadata metadata) {
    Key partitionKey =
        createKeyFromPredicates(statement.predicates, metadata.getPartitionKeyNames());
    Key clusteringKey = null;
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      clusteringKey =
          createKeyFromPredicates(statement.predicates, metadata.getClusteringKeyNames());
    }
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(statement.namespaceName)
        .forTable(statement.tableName);
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

  private static Key createKeyFromPredicates(
      List<Predicate> predicates, Collection<String> keyColumnNames) {
    Map<String, Predicate> predicatesMap =
        predicates.stream()
            .filter(c -> keyColumnNames.contains(c.columnName))
            .collect(Collectors.toMap(a -> a.columnName, Function.identity()));

    Key.Builder builder = Key.newBuilder();
    keyColumnNames.forEach(
        n -> {
          Predicate predicate = predicatesMap.get(n);
          switch (predicate.operator) {
            case EQUAL_TO:
              addToKeyBuilder(builder, n, predicate.value);
              break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL_TO:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL_TO:
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
      default:
        throw new AssertionError();
    }
  }

  private static void addValueToPut(
      Put put, String columnName, Value value, com.scalar.db.api.TableMetadata metadata) {
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
        switch (metadata.getColumnDataType(columnName)) {
          case BOOLEAN:
            put.withBooleanValue(columnName, null);
            break;
          case INT:
            put.withIntValue(columnName, null);
            break;
          case BIGINT:
            put.withBigIntValue(columnName, null);
            break;
          case FLOAT:
            put.withFloatValue(columnName, null);
            break;
          case DOUBLE:
            put.withDoubleValue(columnName, null);
            break;
          case TEXT:
            put.withTextValue(columnName, null);
            break;
          case BLOB:
            put.withBlobValue(columnName, (ByteBuffer) null);
            break;
          default:
            throw new AssertionError();
        }
        break;
      default:
        throw new AssertionError();
    }
  }

  public static com.scalar.db.api.TableMetadata getTableMetadata(
      TableMetadataManager tableMetadataManager, String namespaceName, String tableName) {
    try {
      com.scalar.db.api.TableMetadata metadata =
          tableMetadataManager.getTableMetadata(namespaceName, tableName);
      if (metadata == null) {
        throw new TableNotFoundException(namespaceName, tableName);
      }
      return metadata;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get a table metadata", e);
    }
  }
}
