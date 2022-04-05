package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import com.scalar.db.sql.Predicate.Operator;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.metadata.ColumnMetadata;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.metadata.TableMetadata;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DmlStatement;
import com.scalar.db.sql.statement.DmlStatementVisitor;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DmlStatementExecutor
    implements DmlStatementVisitor<ResultSet, TransactionCrudOperable> {

  private final Metadata metadata;

  DmlStatementExecutor(Metadata metadata) {
    this.metadata = Objects.requireNonNull(metadata);
  }

  public ResultSet execute(TransactionCrudOperable transaction, DmlStatement statement) {
    return statement.accept(this, transaction);
  }

  @Override
  public ResultSet visit(SelectStatement statement, TransactionCrudOperable transaction) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);
    Selection selection = convertSelectStatementToSelection(statement, tableMetadata);

    List<Projection> projections =
        statement.projections.isEmpty()
            ? tableMetadata.getColumns().keySet().stream()
                .map(Projection::column)
                .collect(Collectors.toList())
            : statement.projections;

    try {
      if (selection instanceof Get) {
        Optional<Result> result = transaction.get((Get) selection);
        return result
            .map(
                r ->
                    (ResultSet)
                        new ResultIteratorResultSet(
                            Collections.singletonList(r).iterator(), projections))
            .orElse(EmptyResultSet.INSTANCE);
      } else {
        List<Result> results = transaction.scan((Scan) selection);
        return new ResultIteratorResultSet(results.iterator(), projections);
      }
    } catch (CrudConflictException e) {
      throw new TransactionConflictException("Conflict happened during selecting a record", e);
    } catch (CrudException e) {
      throw new SqlException("Failed to insert a record", e);
    }
  }

  @Override
  public ResultSet visit(InsertStatement statement, TransactionCrudOperable transaction) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);
    Put put = convertInsertStatementToPut(statement, tableMetadata);
    try {
      transaction.put(put);
      return EmptyResultSet.INSTANCE;
    } catch (CrudConflictException e) {
      throw new TransactionConflictException("Conflict happened during inserting a record", e);
    } catch (CrudException e) {
      throw new SqlException("Failed to insert a record", e);
    }
  }

  @Override
  public ResultSet visit(UpdateStatement statement, TransactionCrudOperable transaction) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);
    Put put = convertUpdateStatementToPut(statement, tableMetadata);
    try {
      transaction.put(put);
      return EmptyResultSet.INSTANCE;
    } catch (CrudConflictException e) {
      throw new TransactionConflictException("Conflict happened during updating a record", e);
    } catch (CrudException e) {
      throw new SqlException("Failed to update a record", e);
    }
  }

  @Override
  public ResultSet visit(DeleteStatement statement, TransactionCrudOperable transaction) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);
    Delete delete = convertDeleteStatementToDelete(statement, tableMetadata);
    try {
      transaction.delete(delete);
      return EmptyResultSet.INSTANCE;
    } catch (CrudConflictException e) {
      throw new TransactionConflictException("Conflict happened during deleting a record", e);
    } catch (CrudException e) {
      throw new SqlException("Failed to delete a record", e);
    }
  }

  private Selection convertSelectStatementToSelection(
      SelectStatement statement, TableMetadata tableMetadata) {
    ImmutableListMultimap<String, Predicate> predicatesMap =
        Multimaps.index(statement.predicates, c -> c.columnName);

    List<String> projectedColumnNames =
        statement.projections.stream().map(p -> p.columnName).collect(Collectors.toList());

    if (SqlUtils.isIndexScan(predicatesMap, tableMetadata)) {
      String indexColumnName = predicatesMap.keySet().iterator().next();
      Scan scan =
          new Scan(createKeyFromPredicatesMap(predicatesMap, Stream.of(indexColumnName)))
              .withProjections(projectedColumnNames)
              .forNamespace(statement.namespaceName)
              .forTable(statement.tableName);
      if (statement.limit > 0) {
        scan.withLimit(statement.limit);
      }
      return scan;
    }

    Key partitionKey =
        createKeyFromPredicatesMap(
            predicatesMap, tableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName));

    if (isGet(predicatesMap, tableMetadata)) {
      Key clusteringKey = null;
      if (!tableMetadata.getClusteringKey().isEmpty()) {
        clusteringKey =
            createKeyFromPredicatesMap(
                predicatesMap,
                tableMetadata.getClusteringKey().keySet().stream().map(ColumnMetadata::getName));
      }
      return new Get(partitionKey, clusteringKey)
          .withProjections(projectedColumnNames)
          .forNamespace(statement.namespaceName)
          .forTable(statement.tableName);
    } else {
      Scan scan =
          new Scan(partitionKey)
              .withProjections(projectedColumnNames)
              .forNamespace(statement.namespaceName)
              .forTable(statement.tableName);
      setClusteringKeyRangeForScan(scan, predicatesMap, tableMetadata);
      if (!statement.clusteringOrderings.isEmpty()) {
        statement.clusteringOrderings.forEach(o -> scan.withOrdering(convertOrdering(o)));
      }
      if (statement.limit > 0) {
        scan.withLimit(statement.limit);
      }
      return scan;
    }
  }

  private boolean isGet(
      ImmutableListMultimap<String, Predicate> predicatesMap, TableMetadata tableMetadata) {
    return tableMetadata.getClusteringKey().keySet().stream()
        .map(ColumnMetadata::getName)
        .allMatch(
            n ->
                predicatesMap.containsKey(n)
                    && predicatesMap.get(n).size() == 1
                    && predicatesMap.get(n).get(0).operator == Operator.EQUAL_TO);
  }

  private Key createKeyFromPredicatesMap(
      ImmutableListMultimap<String, Predicate> predicatesMap, Stream<String> keyColumnNamesStream) {
    Key.Builder builder = Key.newBuilder();
    keyColumnNamesStream.forEach(
        n -> {
          Predicate predicate = predicatesMap.get(n).get(0);
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

  private void setClusteringKeyRangeForScan(
      Scan scan,
      ImmutableListMultimap<String, Predicate> predicatesMap,
      TableMetadata tableMetadata) {
    Key.Builder startClusteringKeyBuilder = Key.newBuilder();
    Key.Builder endClusteringKeyBuilder = Key.newBuilder();

    Iterator<String> clusteringKeyNamesIterator =
        tableMetadata.getClusteringKey().keySet().stream().map(ColumnMetadata::getName).iterator();
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

  private Scan.Ordering convertOrdering(ClusteringOrdering clusteringOrdering) {
    switch (clusteringOrdering.clusteringOrder) {
      case ASC:
        return Scan.Ordering.asc(clusteringOrdering.columnName);
      case DESC:
        return Scan.Ordering.desc(clusteringOrdering.columnName);
      default:
        throw new AssertionError();
    }
  }

  private Put convertInsertStatementToPut(InsertStatement statement, TableMetadata tableMetadata) {
    Key partitionKey =
        createKeyFromAssignments(statement.assignments, tableMetadata.getPartitionKey().stream());
    Key clusteringKey = null;
    if (!tableMetadata.getClusteringKey().isEmpty()) {
      clusteringKey =
          createKeyFromAssignments(
              statement.assignments, tableMetadata.getClusteringKey().keySet().stream());
    }
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(statement.namespaceName)
            .forTable(statement.tableName);
    statement.assignments.stream()
        .filter(a -> !tableMetadata.isPrimaryKeyColumn(a.columnName))
        .forEach(a -> addValueToPut(put, a.columnName, a.value, tableMetadata));
    return put;
  }

  private Key createKeyFromAssignments(
      List<Assignment> assignments, Stream<ColumnMetadata> keyColumnStream) {
    Map<String, Assignment> assignmentMap =
        assignments.stream().collect(Collectors.toMap(a -> a.columnName, Function.identity()));

    Key.Builder builder = Key.newBuilder();
    keyColumnStream
        .map(ColumnMetadata::getName)
        .forEach(n -> addToKeyBuilder(builder, n, assignmentMap.get(n).value));
    return builder.build();
  }

  private Put convertUpdateStatementToPut(UpdateStatement statement, TableMetadata tableMetadata) {
    Key partitionKey =
        createKeyFromPredicates(statement.predicates, tableMetadata.getPartitionKey().stream());
    Key clusteringKey = null;
    if (!tableMetadata.getClusteringKey().isEmpty()) {
      clusteringKey =
          createKeyFromPredicates(
              statement.predicates, tableMetadata.getClusteringKey().keySet().stream());
    }
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(statement.namespaceName)
            .forTable(statement.tableName);
    statement.assignments.forEach(a -> addValueToPut(put, a.columnName, a.value, tableMetadata));
    return put;
  }

  private Delete convertDeleteStatementToDelete(
      DeleteStatement statement, TableMetadata tableMetadata) {
    Key partitionKey =
        createKeyFromPredicates(statement.predicates, tableMetadata.getPartitionKey().stream());
    Key clusteringKey = null;
    if (!tableMetadata.getClusteringKey().isEmpty()) {
      clusteringKey =
          createKeyFromPredicates(
              statement.predicates, tableMetadata.getClusteringKey().keySet().stream());
    }
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(statement.namespaceName)
        .forTable(statement.tableName);
  }

  private Key createKeyFromPredicates(
      List<Predicate> predicates, Stream<ColumnMetadata> keyColumnStream) {
    Map<String, Predicate> predicatesMap =
        predicates.stream().collect(Collectors.toMap(a -> a.columnName, Function.identity()));

    Key.Builder builder = Key.newBuilder();
    keyColumnStream
        .map(ColumnMetadata::getName)
        .forEach(
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

  private void addToKeyBuilder(Key.Builder builder, String columnName, Value value) {
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

  private void addValueToPut(Put put, String columnName, Value value, TableMetadata tableMetadata) {
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
        ColumnMetadata column =
            tableMetadata
                .getColumn(columnName)
                .orElseThrow(() -> new SqlException(columnName + " is not found"));
        switch (column.getDataType()) {
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
}
