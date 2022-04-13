package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SimpleSelectQuery implements SelectQuery {

  private final List<String> projections;
  private final RdbEngine rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Optional<Key> commonClusteringKey;
  private final Optional<Column<?>> startColumn;
  private final boolean startInclusive;
  private final Optional<Column<?>> endColumn;
  private final boolean endInclusive;
  private final List<Scan.Ordering> orderings;
  private final boolean isRangeQuery;
  private final Optional<String> indexedColumn;

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
  }

  @Override
  public String sql() {
    return "SELECT "
        + projectionSqlString()
        + " FROM "
        + enclosedFullTableName(schema, table, rdbEngine)
        + " WHERE "
        + conditionSqlString()
        + orderBySqlString();
  }

  private String projectionSqlString() {
    if (projections.isEmpty()) {
      return "*";
    }
    return projections.stream().map(p -> enclose(p, rdbEngine)).collect(Collectors.joining(","));
  }

  private String conditionSqlString() {
    List<String> conditions = new ArrayList<>();
    partitionKey.forEach(v -> conditions.add(enclose(v.getName(), rdbEngine) + "=?"));
    clusteringKey.ifPresent(
        k -> k.forEach(v -> conditions.add(enclose(v.getName(), rdbEngine) + "=?")));
    commonClusteringKey.ifPresent(
        k -> k.forEach(v -> conditions.add(enclose(v.getName(), rdbEngine) + "=?")));
    startColumn.ifPresent(
        c -> conditions.add(enclose(c.getName(), rdbEngine) + (startInclusive ? ">=?" : ">?")));
    endColumn.ifPresent(
        c -> conditions.add(enclose(c.getName(), rdbEngine) + (endInclusive ? "<=?" : "<?")));
    return String.join(" AND ", conditions);
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
            .map(o -> enclose(o.getColumnName(), rdbEngine) + " " + o.getOrder())
            .collect(Collectors.joining(","));
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);

    for (Column<?> column : partitionKey.getColumns()) {
      column.accept(binder);
      binder.throwSQLExceptionIfOccurred();
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
  }
}
