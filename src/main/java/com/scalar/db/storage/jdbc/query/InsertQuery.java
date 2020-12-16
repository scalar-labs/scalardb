package com.scalar.db.storage.jdbc.query;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.Table;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InsertQuery extends AbstractQuery {

  public static class Builder {
    private final Table table;
    private Key partitionKey;
    @Nullable private Key clusteringKey;
    private Map<String, Value> values;

    Builder(Table table) {
      this.table = table;
    }

    public Builder values(
        Key partitionKey, @Nullable Key clusteringKey, Map<String, Value> values) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
      return this;
    }

    public InsertQuery build() {
      if (table == null || partitionKey == null) {
        throw new IllegalStateException("table or partitionKey is null.");
      }
      return new InsertQuery(this);
    }
  }

  private final Table table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  private final Map<String, Value> values;

  private InsertQuery(Builder builder) {
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  protected String sql() {
    return "INSERT INTO " + table + " " + makeValuesSQLString();
  }

  private String makeValuesSQLString() {
    List<String> names = new ArrayList<>();
    for (Value value : partitionKey) {
      names.add(value.getName());
    }
    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
        names.add(value.getName());
      }
    }
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

    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
        value.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    for (Value value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }
}
