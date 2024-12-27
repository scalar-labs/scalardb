package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.getConditionString;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class UpdateQuery implements Query {

  private final RdbEngineStrategy<?, ?, ?, ?> rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;
  private final List<ConditionalExpression> otherConditions;

  private UpdateQuery(Builder builder) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    columns = builder.columns;
    otherConditions = builder.otherConditions;
  }

  @Override
  public String sql() {
    return "UPDATE "
        + rdbEngine.encloseFullTableName(schema, table)
        + " SET "
        + makeSetSqlString()
        + " WHERE "
        + makeConditionSqlString();
  }

  private String makeSetSqlString() {
    return columns.keySet().stream()
        .map(n -> rdbEngine.enclose(n) + "=?")
        .collect(Collectors.joining(","));
  }

  private String makeConditionSqlString() {
    List<String> conditions = new ArrayList<>();
    partitionKey.getColumns().forEach(v -> conditions.add(rdbEngine.enclose(v.getName()) + "=?"));
    clusteringKey.ifPresent(
        k -> k.getColumns().forEach(v -> conditions.add(rdbEngine.enclose(v.getName()) + "=?")));
    otherConditions.forEach(
        c ->
            conditions.add(
                getConditionString(c.getColumn().getName(), c.getOperator(), rdbEngine)));
    return String.join(" AND ", conditions);
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);

    for (Column<?> column : columns.values()) {
      column.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

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

    for (ConditionalExpression condition : otherConditions) {
      // no need to bind for 'is null' and 'is not null' conditions
      if (condition.getOperator() != Operator.IS_NULL
          && condition.getOperator() != Operator.IS_NOT_NULL) {
        condition.getColumn().accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }
  }

  public static class Builder {
    private final RdbEngineStrategy rdbEngine;
    private final String schema;
    private final String table;
    private final TableMetadata tableMetadata;
    List<ConditionalExpression> otherConditions;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private Map<String, Column<?>> columns;

    Builder(
        RdbEngineStrategy<?, ?, ?, ?> rdbEngine,
        String schema,
        String table,
        TableMetadata tableMetadata) {
      this.rdbEngine = rdbEngine;
      this.schema = schema;
      this.table = table;
      this.tableMetadata = tableMetadata;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder set(Map<String, Column<?>> columns) {
      this.columns = columns;
      return this;
    }

    public Builder where(Key partitionKey, Optional<Key> clusteringKey) {
      return where(partitionKey, clusteringKey, Collections.emptyList());
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder where(
        Key partitionKey,
        Optional<Key> clusteringKey,
        List<ConditionalExpression> otherConditions) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.otherConditions = otherConditions;
      return this;
    }

    public UpdateQuery build() {
      return new UpdateQuery(this);
    }
  }
}
