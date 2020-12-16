package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.ResultImpl;
import com.scalar.db.storage.jdbc.Table;
import com.scalar.db.storage.jdbc.metadata.TableMetadata;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleSelectQuery extends AbstractQuery implements SelectQuery {
  private final TableMetadata tableMetadata;
  private final List<String> projections;
  private final Table table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  @Nullable private final Key commonClusteringKey;
  @Nullable private final Value startValue;
  private final boolean startInclusive;
  @Nullable private final Value endValue;
  private final boolean endInclusive;
  private final List<Scan.Ordering> orderings;
  private final boolean isRangeQuery;

  SimpleSelectQuery(Builder builder) {
    tableMetadata = builder.tableMetadata;
    projections = builder.projections;
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    commonClusteringKey = builder.commonClusteringKey;
    startValue = builder.startValue;
    startInclusive = builder.startInclusive;
    endValue = builder.endValue;
    endInclusive = builder.endInclusive;
    orderings = builder.orderings;
    isRangeQuery = builder.isRangeQuery;
  }

  protected String sql() {
    return "SELECT "
        + projectionSQLString()
        + " FROM "
        + table
        + " WHERE "
        + conditionSQLString()
        + orderBySQLString();
  }

  private String projectionSQLString() {
    if (projections.isEmpty()) {
      return "*";
    }
    return String.join(",", projections);
  }

  private String conditionSQLString() {
    List<String> conditions = new ArrayList<>();

    for (Value value : partitionKey) {
      conditions.add(value.getName() + "=?");
    }

    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
        conditions.add(value.getName() + "=?");
      }
    }

    if (commonClusteringKey != null) {
      for (Value value : commonClusteringKey) {
        conditions.add(value.getName() + "=?");
      }
    }

    if (startValue != null) {
      conditions.add(startValue.getName() + (startInclusive ? ">=?" : ">?"));
    }

    if (endValue != null) {
      conditions.add(endValue.getName() + (endInclusive ? "<=?" : "<?"));
    }

    return String.join(" AND ", conditions);
  }

  private String orderBySQLString() {
    if (isRangeQuery) {
      List<Scan.Ordering> orderingList = new ArrayList<>(orderings);

      Boolean reverse = null;
      for (int i = 0; i < tableMetadata.getClusteringKeys().size(); i++) {
        if (i < orderings.size()) {
          Scan.Ordering ordering = orderings.get(i);
          if (reverse == null) {
            reverse =
                ordering.getOrder() != tableMetadata.getClusteringKeyOrder(ordering.getName());
          }
        } else {
          String clusteringKeyName = tableMetadata.getClusteringKeys().get(i);
          Scan.Ordering.Order order = tableMetadata.getClusteringKeyOrder(clusteringKeyName);

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
              .map(o -> o.getName() + " " + o.getOrder())
              .collect(Collectors.joining(","));
    }

    return "";
  }

  @Override
  protected void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder = new PreparedStatementBinder(preparedStatement);

    for (Value value : partitionKey) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
        value.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (commonClusteringKey != null) {
      for (Value value : commonClusteringKey) {
        value.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (startValue != null) {
      startValue.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (endValue != null) {
      endValue.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public Result getResult(ResultSet resultSet) throws SQLException {
    return new ResultImpl(tableMetadata, projections, resultSet);
  }
}
