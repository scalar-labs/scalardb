package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import com.scalar.db.sql.metadata.ColumnMetadata;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.metadata.TableMetadata;
import com.scalar.db.sql.statement.CreateCoordinatorTableStatement;
import com.scalar.db.sql.statement.CreateIndexStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DropCoordinatorTableStatement;
import com.scalar.db.sql.statement.DropIndexStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.Statement;
import com.scalar.db.sql.statement.StatementVisitor;
import com.scalar.db.sql.statement.TruncateCoordinatorTableStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class StatementValidator implements StatementVisitor<Void, Void> {

  private final Metadata metadata;

  public StatementValidator(Metadata metadata) {
    this.metadata = Objects.requireNonNull(metadata);
  }

  public void validate(Statement statement) {
    statement.accept(this, null);
  }

  @Override
  public Void visit(CreateNamespaceStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(CreateTableStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(DropNamespaceStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(DropTableStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(TruncateTableStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(CreateCoordinatorTableStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(DropCoordinatorTableStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(TruncateCoordinatorTableStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(CreateIndexStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(DropIndexStatement statement, Void context) {
    return null;
  }

  @Override
  public Void visit(SelectStatement statement, Void context) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);

    ImmutableListMultimap<String, Predicate> predicatesMap =
        Multimaps.index(statement.predicates, c -> c.columnName);

    // duplication check for projections
    ImmutableSet<Projection> projectionSet = ImmutableSet.copyOf(statement.projections);
    if (statement.projections.size() != projectionSet.size()) {
      throw new IllegalArgumentException(
          "Specifying duplicated projected column names is not allowed");
    }

    // check for an index scan
    if (SqlUtils.isIndexScan(predicatesMap, tableMetadata)) {
      if (!statement.clusteringOrderings.isEmpty()) {
        throw new IllegalArgumentException("Specifying 'order by' is not allowed in an index scan");
      }
      return null;
    }

    // check if only primary key columns are specified
    predicatesMap
        .keys()
        .forEach(
            c -> {
              if (!tableMetadata.isPrimaryKeyColumn(c)) {
                throw new IllegalArgumentException(
                    "A column, " + c + " is not a primary key column");
              }
            });

    validatePredicatesForPartitionKey(predicatesMap, tableMetadata);
    validatePredicatesForClusteringKey(predicatesMap, tableMetadata);
    return null;
  }

  private void validatePredicatesForPartitionKey(
      ImmutableListMultimap<String, Predicate> predicatesMap, TableMetadata tableMetadata) {
    // check if all columns of the partition key are specified properly
    boolean areAllPartitionKeyColumnsSpecifiedProperly =
        tableMetadata.getPartitionKey().stream()
            .map(ColumnMetadata::getName)
            .allMatch(
                n ->
                    predicatesMap.get(n).size() == 1
                        && predicatesMap.get(n).get(0).operator == Predicate.Operator.EQUAL_TO
                        && predicatesMap.get(n).get(0).value.type != Value.Type.NULL);
    if (!areAllPartitionKeyColumnsSpecifiedProperly) {
      throw new IllegalArgumentException("Partition key columns are not specified properly");
    }
  }

  private void validatePredicatesForClusteringKey(
      ImmutableListMultimap<String, Predicate> predicatesMap, TableMetadata tableMetadata) {
    boolean finished = false;
    for (ColumnMetadata column : tableMetadata.getClusteringKey().keySet()) {
      ImmutableList<Predicate> predicates = predicatesMap.get(column.getName());
      if (predicates.isEmpty()) {
        finished = true;
        continue;
      }
      if (finished) {
        throw new IllegalArgumentException("Clustering key columns are not specified properly");
      }

      if (predicates.size() > 2) {
        throw new IllegalArgumentException("Clustering key columns are not specified properly");
      } else if (predicates.size() == 2) {
        Predicate predicate1 = predicates.get(0);
        Predicate predicate2 = predicates.get(1);

        if (predicate1.operator == Predicate.Operator.EQUAL_TO
            || predicate2.operator == Predicate.Operator.EQUAL_TO) {
          throw new IllegalArgumentException("Clustering key columns are not specified properly");
        }
        if ((predicate1.operator == Predicate.Operator.GREATER_THAN
                || predicate1.operator == Predicate.Operator.GREATER_THAN_OR_EQUAL_TO)
            && (predicate2.operator == Predicate.Operator.GREATER_THAN
                || predicate2.operator == Predicate.Operator.GREATER_THAN_OR_EQUAL_TO)) {
          throw new IllegalArgumentException("Clustering key columns are not specified properly");
        }
        if ((predicate1.operator == Predicate.Operator.LESS_THAN
                || predicate1.operator == Predicate.Operator.LESS_THAN_OR_EQUAL_TO)
            && (predicate2.operator == Predicate.Operator.LESS_THAN
                || predicate2.operator == Predicate.Operator.LESS_THAN_OR_EQUAL_TO)) {
          throw new IllegalArgumentException("Clustering key columns are not specified properly");
        }
        finished = true;
      } else {
        if (predicates.get(0).operator != Predicate.Operator.EQUAL_TO) {
          finished = true;
        }
      }
    }
  }

  @Override
  public Void visit(InsertStatement statement, Void context) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);

    Map<String, Assignment> assignmentsMap = new HashMap<>(statement.assignments.size());
    statement.assignments.forEach(
        p -> {
          // check if each column is specified only once
          if (assignmentsMap.containsKey(p.columnName)) {
            throw new IllegalArgumentException(
                "A column, " + p.columnName + " is specified duplicately");
          }
          assignmentsMap.put(p.columnName, p);
        });

    // check if all columns of the primary key are specified properly
    boolean areAllPrimaryKeyColumnsSpecifiedProperly =
        Streams.concat(
                tableMetadata.getPartitionKey().stream(),
                tableMetadata.getClusteringKey().keySet().stream())
            .map(ColumnMetadata::getName)
            .allMatch(
                n ->
                    assignmentsMap.containsKey(n)
                        && assignmentsMap.get(n).value.type != Value.Type.NULL);
    if (!areAllPrimaryKeyColumnsSpecifiedProperly) {
      throw new IllegalArgumentException("Primary key columns are not specified properly");
    }
    return null;
  }

  @Override
  public Void visit(UpdateStatement statement, Void context) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);
    validatePredicatesForPrimaryKey(statement.predicates, tableMetadata);
    return null;
  }

  @Override
  public Void visit(DeleteStatement statement, Void context) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(metadata, statement.namespaceName, statement.tableName);
    validatePredicatesForPrimaryKey(statement.predicates, tableMetadata);
    return null;
  }

  private void validatePredicatesForPrimaryKey(
      ImmutableList<Predicate> predicates, TableMetadata tableMetadata) {
    Map<String, Predicate> predicatesMap = new HashMap<>(predicates.size());
    predicates.forEach(
        p -> {
          // check if only primary key columns are specified
          if (!tableMetadata.isPrimaryKeyColumn(p.columnName)) {
            throw new IllegalArgumentException(
                "A column, " + p.columnName + " is not a primary key column");
          }

          // check if each column is specified only once
          if (predicatesMap.containsKey(p.columnName)) {
            throw new IllegalArgumentException(
                "A column, " + p.columnName + " is specified duplicately");
          }
          predicatesMap.put(p.columnName, p);
        });

    // check if all columns of the primary key are specified properly
    boolean areAllPrimaryKeyColumnsSpecifiedProperly =
        Streams.concat(
                tableMetadata.getPartitionKey().stream(),
                tableMetadata.getClusteringKey().keySet().stream())
            .map(ColumnMetadata::getName)
            .allMatch(
                n ->
                    predicatesMap.containsKey(n)
                        && predicatesMap.get(n).operator == Predicate.Operator.EQUAL_TO
                        && predicatesMap.get(n).value.type != Value.Type.NULL);
    if (!areAllPrimaryKeyColumnsSpecifiedProperly) {
      throw new IllegalArgumentException("Primary key columns are not specified properly");
    }
  }

  @Override
  public Void visit(Statement statement, Void context) {
    return null;
  }
}
