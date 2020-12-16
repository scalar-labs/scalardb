package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.Table;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.scalar.db.storage.jdbc.query.QueryUtils.getOperatorString;

public class DeleteQuery extends AbstractQuery {

  public static class Builder {
    private final Table table;
    private Key partitionKey;
    @Nullable private Key clusteringKey;
    @Nullable private List<ConditionalExpression> otherConditions;

    Builder(Table table) {
      this.table = table;
    }

    public Builder where(Key partitionKey, @Nullable Key clusteringKey) {
      return where(partitionKey, clusteringKey, null);
    }

    public Builder where(
        Key partitionKey,
        @Nullable Key clusteringKey,
        @Nullable List<ConditionalExpression> otherConditions) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.otherConditions = otherConditions;
      return this;
    }

    public DeleteQuery build() {
      if (table == null || partitionKey == null) {
        throw new IllegalStateException("table or partitionKey is null.");
      }
      return new DeleteQuery(this);
    }
  }

  private final Table table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  @Nullable private final List<ConditionalExpression> otherConditions;

  private DeleteQuery(Builder builder) {
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    otherConditions = builder.otherConditions;
  }

  protected String sql() {
    return "DELETE FROM " + table + " WHERE " + conditionSQLString();
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

    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
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
