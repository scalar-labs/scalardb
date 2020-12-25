package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.scalar.db.storage.jdbc.query.QueryUtils.getOperatorString;

public class DeleteQuery extends AbstractQuery {

  private final String fullTableName;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final List<ConditionalExpression> otherConditions;

  private DeleteQuery(Builder builder) {
    fullTableName = builder.fullTableName;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    otherConditions = builder.otherConditions;
  }

  protected String sql() {
    return "DELETE FROM " + fullTableName + " WHERE " + conditionSqlString();
  }

  private String conditionSqlString() {
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

    if (!otherConditions.isEmpty()) {
      for (ConditionalExpression condition : otherConditions) {
        condition.getValue().accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }
  }

  public static class Builder {
    private final String fullTableName;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private List<ConditionalExpression> otherConditions;

    Builder(String fullTableName) {
      this.fullTableName = fullTableName;
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

    public DeleteQuery build() {
      return new DeleteQuery(this);
    }
  }
}
