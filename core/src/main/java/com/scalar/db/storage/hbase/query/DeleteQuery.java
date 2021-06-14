package com.scalar.db.storage.hbase.query;

import static com.scalar.db.storage.hbase.query.QueryUtils.enclose;
import static com.scalar.db.storage.hbase.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.storage.hbase.query.QueryUtils.getOperatorString;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DeleteQuery extends AbstractQuery {

  private final String schema;
  private final String table;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final List<ConditionalExpression> otherConditions;

  private DeleteQuery(Builder builder) {
    schema = builder.schema;
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    otherConditions = builder.otherConditions;
  }

  protected String sql() {
    return "DELETE FROM " + enclosedFullTableName(schema, table) + " WHERE " + conditionSqlString();
  }

  private String conditionSqlString() {
    List<String> conditions = new ArrayList<>();
    conditions.add(enclose(HASH_COLUMN_NAME) + "=?");
    partitionKey.forEach(v -> conditions.add(enclose(v.getName()) + "=?"));
    clusteringKey.ifPresent(k -> k.forEach(v -> conditions.add(enclose(v.getName()) + "=?")));
    otherConditions.forEach(
        c -> conditions.add(enclose(c.getName()) + getOperatorString(c.getOperator()) + "?"));
    return String.join(" AND ", conditions);
  }

  @Override
  protected void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder = new PreparedStatementBinder(preparedStatement);

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

    for (ConditionalExpression condition : otherConditions) {
      condition.getValue().accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {
    private final String schema;
    private final String table;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private List<ConditionalExpression> otherConditions;

    Builder(String schema, String table) {
      this.schema = schema;
      this.table = table;
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
