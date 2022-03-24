package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
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
public class StatementValidator implements StatementVisitor {

  private final TableMetadataManager tableMetadataManager;

  public StatementValidator(TableMetadataManager tableMetadataManager) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
  }

  public void validate(Statement statement) {
    statement.accept(this);
  }

  @Override
  public void visit(CreateNamespaceStatement statement) {}

  @Override
  public void visit(CreateTableStatement statement) {}

  @Override
  public void visit(DropNamespaceStatement statement) {}

  @Override
  public void visit(DropTableStatement statement) {}

  @Override
  public void visit(TruncateTableStatement statement) {}

  @Override
  public void visit(CreateCoordinatorTableStatement statement) {}

  @Override
  public void visit(DropCoordinatorTableStatement statement) {}

  @Override
  public void visit(TruncateCoordinatorTableStatement statement) {}

  @Override
  public void visit(CreateIndexStatement statement) {}

  @Override
  public void visit(DropIndexStatement statement) {}

  @Override
  public void visit(SelectStatement statement) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(
            tableMetadataManager, statement.namespaceName, statement.tableName);

    ImmutableListMultimap<String, Predicate> predicatesMap =
        Multimaps.index(statement.predicates, c -> c.columnName);

    // check if only primary key columns are specified
    predicatesMap
        .keys()
        .forEach(
            c -> {
              if (!tableMetadata.getPartitionKeyNames().contains(c)
                  && !tableMetadata.getClusteringKeyNames().contains(c)) {
                throw new IllegalArgumentException(
                    "A column, " + c + " is not a primary key column");
              }
            });

    validatePredicatesForPartitionKey(predicatesMap, tableMetadata);
    validatePredicatesForClusteringKey(predicatesMap, tableMetadata);
  }

  private void validatePredicatesForPartitionKey(
      ImmutableListMultimap<String, Predicate> predicatesMap, TableMetadata tableMetadata) {
    // check if all columns of the partition key are specified properly
    boolean areAllPartitionKeyColumnsSpecifiedProperly =
        tableMetadata.getPartitionKeyNames().stream()
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
    for (String clusteringKeyName : tableMetadata.getClusteringKeyNames()) {
      ImmutableList<Predicate> predicates = predicatesMap.get(clusteringKeyName);
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
  public void visit(InsertStatement statement) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(
            tableMetadataManager, statement.namespaceName, statement.tableName);

    Map<String, Assignment> assignmentsMap = new HashMap<>();
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
                tableMetadata.getPartitionKeyNames().stream(),
                tableMetadata.getClusteringKeyNames().stream())
            .allMatch(
                n ->
                    assignmentsMap.containsKey(n)
                        && assignmentsMap.get(n).value.type != Value.Type.NULL);
    if (!areAllPrimaryKeyColumnsSpecifiedProperly) {
      throw new IllegalArgumentException("Primary key columns are not specified properly");
    }
  }

  @Override
  public void visit(UpdateStatement statement) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(
            tableMetadataManager, statement.namespaceName, statement.tableName);
    validatePredicatesForPrimaryKey(statement.predicates, tableMetadata);
  }

  @Override
  public void visit(DeleteStatement statement) {
    TableMetadata tableMetadata =
        SqlUtils.getTableMetadata(
            tableMetadataManager, statement.namespaceName, statement.tableName);
    validatePredicatesForPrimaryKey(statement.predicates, tableMetadata);
  }

  private void validatePredicatesForPrimaryKey(
      ImmutableList<Predicate> predicates, TableMetadata tableMetadata) {
    Map<String, Predicate> predicatesMap = new HashMap<>();
    predicates.forEach(
        p -> {
          // check if only primary key columns are specified
          if (!tableMetadata.getPartitionKeyNames().contains(p.columnName)
              && !tableMetadata.getClusteringKeyNames().contains(p.columnName)) {
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
                tableMetadata.getPartitionKeyNames().stream(),
                tableMetadata.getClusteringKeyNames().stream())
            .allMatch(
                n ->
                    predicatesMap.containsKey(n)
                        && predicatesMap.get(n).operator == Predicate.Operator.EQUAL_TO
                        && predicatesMap.get(n).value.type != Value.Type.NULL);
    if (!areAllPrimaryKeyColumnsSpecifiedProperly) {
      throw new IllegalArgumentException("Primary key columns are not specified properly");
    }
  }
}
