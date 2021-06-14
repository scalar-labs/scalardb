package com.scalar.db.storage.hbase.query;

import static com.scalar.db.storage.hbase.query.QueryUtils.enclose;
import static com.scalar.db.storage.hbase.query.QueryUtils.enclosedFullTableName;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.hbase.HBaseTableMetadataManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SelectQuery extends AbstractQuery {

  private final TableMetadata tableMetadata;
  private final List<String> projections;
  private final String schema;
  private final String table;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Optional<Key> commonClusteringKey;
  private final Optional<Value<?>> startValue;
  private final boolean startInclusive;
  private final Optional<Value<?>> endValue;
  private final boolean endInclusive;
  private final List<Scan.Ordering> orderings;
  private final boolean isRangeQuery;
  private final Optional<Value<?>> indexedColumn;
  private final int limit;

  private SelectQuery(Builder builder) {
    tableMetadata = builder.tableMetadata;
    projections = builder.projections;
    schema = builder.schema;
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
    indexedColumn = builder.indexedColumn;
    limit = builder.limit;
  }

  @Override
  protected String sql() {
    return "SELECT "
        + projectionSqlString()
        + " FROM "
        + enclosedFullTableName(schema, table)
        + " WHERE "
        + conditionSqlString()
        + orderBySqlString()
        + (limit > 0 ? " LIMIT " + limit : "");
  }

  private String projectionSqlString() {
    if (projections.isEmpty()) {
      return "*";
    }
    return projections.stream().map(QueryUtils::enclose).collect(Collectors.joining(","));
  }

  private String conditionSqlString() {
    if (indexedColumn.isPresent()) {
      return enclose(indexedColumn.get().getName()) + "=?";
    }

    List<String> conditions = new ArrayList<>();
    conditions.add(enclose(HASH_COLUMN_NAME) + "=?");
    partitionKey.forEach(v -> conditions.add(enclose(v.getName()) + "=?"));
    clusteringKey.ifPresent(k -> k.forEach(v -> conditions.add(enclose(v.getName()) + "=?")));
    commonClusteringKey.ifPresent(k -> k.forEach(v -> conditions.add(enclose(v.getName()) + "=?")));
    startValue.ifPresent(
        sv -> conditions.add(enclose(sv.getName()) + (startInclusive ? ">=?" : ">?")));
    endValue.ifPresent(ev -> conditions.add(enclose(ev.getName()) + (endInclusive ? "<=?" : "<?")));
    return String.join(" AND ", conditions);
  }

  private String orderBySqlString() {
    if (!isRangeQuery || indexedColumn.isPresent()) {
      return "";
    }

    List<Scan.Ordering> orderingList = new ArrayList<>(orderings);

    Boolean reverse = null;
    int i = 0;
    for (String clusteringKeyName : tableMetadata.getClusteringKeyNames()) {
      if (i < orderings.size()) {
        Scan.Ordering ordering = orderings.get(i++);
        if (reverse == null) {
          reverse = ordering.getOrder() != tableMetadata.getClusteringOrder(ordering.getName());
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
            .map(o -> enclose(o.getName()) + " " + o.getOrder())
            .collect(Collectors.joining(","));
  }

  @Override
  protected void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder = new PreparedStatementBinder(preparedStatement);

    if (indexedColumn.isPresent()) {
      indexedColumn.get().accept(binder);
      binder.throwSQLExceptionIfOccurred();
      return;
    }

    binder.setInt(hash(partitionKey));
    binder.throwSQLExceptionIfOccurred();

    for (Value<?> value : partitionKey) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (clusteringKey.isPresent()) {
      for (Value<?> value : clusteringKey.get()) {
        value.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (commonClusteringKey.isPresent()) {
      for (Value<?> value : commonClusteringKey.get()) {
        value.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (startValue.isPresent()) {
      startValue.get().accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (endValue.isPresent()) {
      endValue.get().accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {

    private final HBaseTableMetadataManager tableMetadataManager;
    final List<String> projections;
    String schema;
    String table;
    TableMetadata tableMetadata;
    Key partitionKey;
    Optional<Key> clusteringKey = Optional.empty();
    Optional<Key> commonClusteringKey = Optional.empty();
    Optional<Value<?>> startValue = Optional.empty();
    boolean startInclusive;
    Optional<Value<?>> endValue = Optional.empty();
    boolean endInclusive;
    List<Scan.Ordering> orderings = Collections.emptyList();
    private int limit;
    boolean isRangeQuery;
    Optional<Value<?>> indexedColumn = Optional.empty();

    Builder(HBaseTableMetadataManager tableMetadataManager, List<String> projections) {
      this.tableMetadataManager = tableMetadataManager;
      this.projections = projections;
    }

    public Builder from(String schema, String table) {
      this.schema = schema;
      this.table = table;

      String fullTableName = schema + "." + table;
      tableMetadata = tableMetadataManager.getTableMetadata(fullTableName);
      return this;
    }

    /*
     * Assumes this is called by get operations
     */
    public Builder where(Key partitionKey, Optional<Key> clusteringKey) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      setIndexInfoIfUsed(partitionKey);
      return this;
    }

    /*
     * Assumes this is called by scan operations
     */
    public Builder where(
        Key partitionKey,
        Optional<Key> startClusteringKey,
        boolean startInclusive,
        Optional<Key> endClusteringKey,
        boolean endInclusive) {
      this.partitionKey = partitionKey;

      if (startClusteringKey.isPresent()) {
        commonClusteringKey =
            Optional.of(
                new Key(
                    startClusteringKey
                        .get()
                        .get()
                        .subList(0, startClusteringKey.get().size() - 1)));
      } else if (endClusteringKey.isPresent()) {
        commonClusteringKey =
            Optional.of(
                new Key(
                    endClusteringKey.get().get().subList(0, endClusteringKey.get().size() - 1)));
      }

      if (startClusteringKey.isPresent()) {
        startValue =
            Optional.of(startClusteringKey.get().get().get(startClusteringKey.get().size() - 1));
        this.startInclusive = startInclusive;
      }

      if (endClusteringKey.isPresent()) {
        endValue = Optional.of(endClusteringKey.get().get().get(endClusteringKey.get().size() - 1));
        this.endInclusive = endInclusive;
      }

      isRangeQuery = true;
      setIndexInfoIfUsed(partitionKey);

      return this;
    }

    private void setIndexInfoIfUsed(Key partitionKey) {
      if (tableMetadata == null) {
        throw new IllegalStateException("tableMetadata is null");
      }

      if (partitionKey.size() > 1) {
        return;
      }

      Value<?> value = partitionKey.get().get(0);
      if (!tableMetadata.getSecondaryIndexNames().contains(value.getName())) {
        return;
      }

      indexedColumn = Optional.of(value);
    }

    public Builder orderBy(List<Scan.Ordering> orderings) {
      this.orderings = orderings;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public SelectQuery build() {
      return new SelectQuery(this);
    }
  }
}
