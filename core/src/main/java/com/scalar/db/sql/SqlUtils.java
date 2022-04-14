package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.scalar.db.sql.Predicate.Operator;
import com.scalar.db.sql.exception.TableNotFoundException;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.metadata.TableMetadata;
import java.util.Iterator;
import java.util.Map;

public final class SqlUtils {

  private SqlUtils() {}

  public static boolean isIndexScan(
      ImmutableListMultimap<String, Predicate> predicatesMap, TableMetadata tableMetadata) {
    if (predicatesMap.size() != 1) {
      return false;
    }
    String columnName = predicatesMap.keySet().iterator().next();
    if (!tableMetadata.getIndex(columnName).isPresent()) {
      return false;
    }
    ImmutableList<Predicate> predicates = predicatesMap.get(columnName);
    if (predicates.size() != 1) {
      return false;
    }
    return predicates.get(0).operator == Operator.EQUAL_TO;
  }

  public static TableMetadata getTableMetadata(
      Metadata metadata, String namespaceName, String tableName) {
    return metadata
        .getNamespace(namespaceName)
        .orElseThrow(() -> new TableNotFoundException(namespaceName, tableName))
        .getTable(tableName)
        .orElseThrow(() -> new TableNotFoundException(namespaceName, tableName));
  }

  public static ImmutableList<Predicate> bindPredicates(
      ImmutableList<Predicate> predicates, Iterator<Value> positionalValueIterator) {
    ImmutableList.Builder<Predicate> builder = ImmutableList.builder();
    predicates.forEach(
        p -> {
          if (positionalValueIterator.hasNext() && p.value instanceof BindMarker) {
            if (p.value instanceof NamedBindMarker) {
              throw new IllegalArgumentException("A named bind marker is not allowed");
            }
            builder.add(p.replaceValue(positionalValueIterator.next()));
          } else {
            builder.add(p);
          }
        });
    return builder.build();
  }

  public static ImmutableList<Predicate> bindPredicates(
      ImmutableList<Predicate> predicates, Map<String, Value> namedValues) {
    ImmutableList.Builder<Predicate> builder = ImmutableList.builder();
    predicates.forEach(
        p -> {
          if (p.value instanceof BindMarker) {
            if (p.value instanceof PositionalBindMarker) {
              throw new IllegalArgumentException("A positional bind marker is not allowed");
            }
            String name = ((NamedBindMarker) p.value).name;
            if (namedValues.containsKey(name)) {
              builder.add(p.replaceValue(namedValues.get(name)));
            } else {
              builder.add(p);
            }
          } else {
            builder.add(p);
          }
        });
    return builder.build();
  }

  public static ImmutableList<Assignment> bindAssignments(
      ImmutableList<Assignment> assignments, Iterator<Value> positionalValueIterator) {
    ImmutableList.Builder<Assignment> builder = ImmutableList.builder();
    assignments.forEach(
        a -> {
          if (positionalValueIterator.hasNext() && a.value instanceof BindMarker) {
            if (a.value instanceof NamedBindMarker) {
              throw new IllegalArgumentException("a named bind marker is not allowed");
            }
            builder.add(a.replaceValue(positionalValueIterator.next()));
          } else {
            builder.add(a);
          }
        });
    return builder.build();
  }

  public static ImmutableList<Assignment> bindAssignments(
      ImmutableList<Assignment> assignments, Map<String, Value> namedValues) {
    ImmutableList.Builder<Assignment> builder = ImmutableList.builder();
    assignments.forEach(
        a -> {
          if (a.value instanceof BindMarker) {
            if (a.value instanceof PositionalBindMarker) {
              throw new IllegalArgumentException("a positional bind marker is not allowed");
            }
            String name = ((NamedBindMarker) a.value).name;
            if (namedValues.containsKey(name)) {
              builder.add(a.replaceValue(namedValues.get(name)));
            } else {
              builder.add(a);
            }
          } else {
            builder.add(a);
          }
        });
    return builder.build();
  }
}
