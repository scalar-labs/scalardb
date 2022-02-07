package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.storage.jdbc.query.QueryUtils.getOperatorString;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RdbEngine;
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

  private final RdbEngine rdbEngine;
  private final String schema;
  private final String table;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value<?>> values;
  private final List<ConditionalExpression> otherConditions;

  private UpdateQuery(Builder builder) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
    otherConditions = builder.otherConditions;
  }

  @Override
  public String sql() {
    return "UPDATE "
        + enclosedFullTableName(schema, table, rdbEngine)
        + " SET "
        + makeSetSqlString()
        + " WHERE "
        + makeConditionSqlString();
  }

  private String makeSetSqlString() {
    return values.keySet().stream()
        .map(n -> enclose(n, rdbEngine) + "=?")
        .collect(Collectors.joining(","));
  }

  private String makeConditionSqlString() {
    List<String> conditions = new ArrayList<>();
    partitionKey.forEach(v -> conditions.add(enclose(v.getName(), rdbEngine) + "=?"));
    clusteringKey.ifPresent(
        k -> k.forEach(v -> conditions.add(enclose(v.getName(), rdbEngine) + "=?")));
    otherConditions.forEach(
        c ->
            conditions.add(
                enclose(c.getName(), rdbEngine) + getOperatorString(c.getOperator()) + "?"));
    return String.join(" AND ", conditions);
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder = new PreparedStatementBinder(preparedStatement);

    for (Value<?> value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

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

    for (ConditionalExpression condition : otherConditions) {
      condition.getValue().accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {
    private final RdbEngine rdbEngine;
    private final String schema;
    private final String table;
    List<ConditionalExpression> otherConditions;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private Map<String, Value<?>> values;

    Builder(RdbEngine rdbEngine, String schema, String table) {
      this.rdbEngine = rdbEngine;
      this.schema = schema;
      this.table = table;
    }

    public Builder set(Map<String, Value<?>> values) {
      this.values = values;
      return this;
    }

    public Builder where(Key partitionKey, Optional<Key> clusteringKey) {
      return where(partitionKey, clusteringKey, Collections.emptyList());
    }

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
