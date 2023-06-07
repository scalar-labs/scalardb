package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SimpleSelectQuery implements SelectQuery {

  private final List<String> projections;
  private final RdbEngineStrategy rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Optional<Key> partitionKey;
  private final Optional<Key> clusteringKey;
  private final Optional<Key> commonClusteringKey;
  private final Optional<Column<?>> startColumn;
  private final boolean startInclusive;
  private final Optional<Column<?>> endColumn;
  private final boolean endInclusive;
  private final List<Scan.Ordering> orderings;
  private final boolean isRangeQuery;
  private final Optional<String> indexedColumn;
  private final boolean isConditionalQuery;
  private final boolean isRelationalQuery;
  private final Set<Conjunction> conjunctions;

  SimpleSelectQuery(Builder builder) {
    projections = builder.projections;
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    commonClusteringKey = builder.commonClusteringKey;
    startColumn = builder.startColumn;
    startInclusive = builder.startInclusive;
    endColumn = builder.endColumn;
    endInclusive = builder.endInclusive;
    orderings = builder.orderings;
    isRangeQuery = builder.isRangeQuery;
    indexedColumn = builder.indexedColumn;
    isConditionalQuery = builder.isConditionalQuery;
    isRelationalQuery = builder.isRelationalQuery;
    conjunctions = builder.conjunctions;
  }

  @Override
  public String sql() {
    String sql =
        "SELECT "
            + projectionSqlString()
            + " FROM "
            + rdbEngine.encloseFullTableName(schema, table);
    if (isRelationalQuery) {
      // for relational abstraction
      sql += relationalConditionSqlString();
      sql += relationalOrderBySqlString();
    } else {
      // for multi-dimensional map abstraction
      if (isConditionalQuery) {
        sql += " WHERE " + conditionSqlString();
      }
      sql += orderBySqlString();
    }

    return sql;
  }

  private String projectionSqlString() {
    if (projections.isEmpty()) {
      return "*";
    }
    return projections.stream().map(rdbEngine::enclose).collect(Collectors.joining(","));
  }

  private String conditionSqlString() {
    List<String> conditions = new ArrayList<>();
    partitionKey.ifPresent(
        k -> k.forEach(v -> conditions.add(rdbEngine.enclose(v.getName()) + "=?")));
    clusteringKey.ifPresent(
        k -> k.forEach(v -> conditions.add(rdbEngine.enclose(v.getName()) + "=?")));
    commonClusteringKey.ifPresent(
        k -> k.forEach(v -> conditions.add(rdbEngine.enclose(v.getName()) + "=?")));
    startColumn.ifPresent(
        c -> conditions.add(rdbEngine.enclose(c.getName()) + (startInclusive ? ">=?" : ">?")));
    endColumn.ifPresent(
        c -> conditions.add(rdbEngine.enclose(c.getName()) + (endInclusive ? "<=?" : "<?")));
    return String.join(" AND ", conditions);
  }

  private String relationalConditionSqlString() {
    if (conjunctions.isEmpty()) {
      return "";
    }

    List<String> conjunctionList = new ArrayList<>();
    conjunctions.forEach(
        conjunction -> {
          List<String> conditions = new ArrayList<>();
          conjunction
              .getConditions()
              .forEach(
                  condition ->
                      conditions.add(
                          rdbEngine.enclose(condition.getColumn().getName())
                              + convert(condition.getOperator())));
          conjunctionList.add(String.join(" AND ", conditions));
        });

    return " WHERE " + String.join(" OR ", conjunctionList);
  }

  private String convert(ConditionalExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return "=?";
      case NE:
        return "!=?";
      case GT:
        return ">?";
      case GTE:
        return ">=?";
      case LT:
        return "<?";
      case LTE:
        return "<=?";
      case IS_NULL:
        return " IS NULL";
      case IS_NOT_NULL:
        return " IS NOT NULL";
      default:
        throw new IllegalArgumentException("unknown operator");
    }
  }

  private String orderBySqlString() {
    if (!isRangeQuery
        || indexedColumn.isPresent()
        || tableMetadata.getClusteringKeyNames().isEmpty()) {
      return "";
    }

    List<Scan.Ordering> orderingList = new ArrayList<>(orderings);

    Boolean reverse = null;
    int i = 0;
    for (String clusteringKeyName : tableMetadata.getClusteringKeyNames()) {
      if (i < orderings.size()) {
        Scan.Ordering ordering = orderings.get(i++);
        if (reverse == null) {
          reverse =
              ordering.getOrder() != tableMetadata.getClusteringOrder(ordering.getColumnName());
        }
      } else {
        Scan.Ordering.Order order = tableMetadata.getClusteringOrder(clusteringKeyName);

        if (reverse != null && reverse) {
          if (order == Scan.Ordering.Order.ASC) {
            order = Scan.Ordering.Order.DESC;
          } else {
            order = Scan.Ordering.Order.ASC;
          }
        }
        orderingList.add(new Scan.Ordering(clusteringKeyName, order));
      }
    }

    return " ORDER BY "
        + orderingList.stream()
            .map(o -> rdbEngine.enclose(o.getColumnName()) + " " + o.getOrder())
            .collect(Collectors.joining(","));
  }

  private String relationalOrderBySqlString() {
    if (orderings.size() == 0) {
      return "";
    }

    return " ORDER BY "
        + orderings.stream()
            .map(o -> rdbEngine.enclose(o.getColumnName()) + " " + o.getOrder())
            .collect(Collectors.joining(","));
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);
    if (partitionKey.isPresent()) {
      for (Column<?> column : partitionKey.get().getColumns()) {
        column.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (clusteringKey.isPresent()) {
      for (Column<?> column : clusteringKey.get().getColumns()) {
        column.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (commonClusteringKey.isPresent()) {
      for (Column<?> column : commonClusteringKey.get().getColumns()) {
        column.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (startColumn.isPresent()) {
      startColumn.get().accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (endColumn.isPresent()) {
      endColumn.get().accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    for (Conjunction conjunction : conjunctions) {
      for (ConditionalExpression condition : conjunction.getConditions()) {
        if (!condition.getColumn().hasNullValue()) {
          condition.getColumn().accept(binder);
        }
        binder.throwSQLExceptionIfOccurred();
      }
    }
  }
}
