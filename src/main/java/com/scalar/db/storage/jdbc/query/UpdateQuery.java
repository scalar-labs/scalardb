package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.scalar.db.storage.jdbc.query.QueryUtils.getOperatorString;

public class UpdateQuery extends AbstractQuery {

  public static class Builder {
    private final String fullTableName;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private Map<String, Value> values;
    List<ConditionalExpression> otherConditions;

    Builder(String fullTableName) {
      this.fullTableName = fullTableName;
    }

    public Builder set(Map<String, Value> values) {
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

  private final String fullTableName;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value> values;
  private final List<ConditionalExpression> otherConditions;

  private UpdateQuery(Builder builder) {
    fullTableName = builder.fullTableName;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
    otherConditions = builder.otherConditions;
  }

  @Override
  protected String sql() {
    return "UPDATE "
        + fullTableName
        + " SET "
        + makeSetSqlString()
        + " WHERE "
        + makeConditionSqlString();
  }

  private String makeSetSqlString() {
    return values.keySet().stream().map(n -> n + "=?").collect(Collectors.joining(","));
  }

  private String makeConditionSqlString() {
    List<String> conditions = new ArrayList<>();

    for (Value value : partitionKey) {
      conditions.add(value.getName() + "=?");
    }

    clusteringKey.ifPresent(ckey -> ckey.forEach(v -> conditions.add(v.getName() + "=?")));

    if (otherConditions != null) {
      for (ConditionalExpression condition : otherConditions) {
        conditions.add(condition.getName() + getOperatorString(condition.getOperator()) + "?");
      }
    }

    return String.join(" AND ", conditions);
  }

  @Override
  protected void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder = new PreparedStatementBinder(preparedStatement);

    for (Value value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    for (Value value : partitionKey) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (clusteringKey.isPresent()) {
      for (Value value : clusteringKey.get()) {
        value.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    if (otherConditions != null) {
      for (ConditionalExpression condition : otherConditions) {
        condition.getValue().accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }
  }
}
