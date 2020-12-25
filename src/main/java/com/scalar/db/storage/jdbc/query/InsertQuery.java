package com.scalar.db.storage.jdbc.query;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class InsertQuery extends AbstractQuery {

  private final String fullTableName;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value> values;

  private InsertQuery(Builder builder) {
    fullTableName = builder.fullTableName;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  protected String sql() {
    return "INSERT INTO " + fullTableName + " " + makeValuesSqlString();
  }

  private String makeValuesSqlString() {
    List<String> names = new ArrayList<>();
    for (Value value : partitionKey) {
      names.add(value.getName());
    }

    clusteringKey.ifPresent(ckey -> ckey.forEach(v -> names.add(v.getName())));

    names.addAll(values.keySet());

    return "("
        + String.join(",", names)
        + ") VALUES("
        + names.stream().map(n -> "?").collect(Collectors.joining(","))
        + ")";
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

    for (Value value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {
    private final String fullTableName;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private Map<String, Value> values;

    Builder(String fullTableName) {
      this.fullTableName = fullTableName;
    }

    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Value> values) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
      return this;
    }

    public InsertQuery build() {
      return new InsertQuery(this);
    }
  }
}
