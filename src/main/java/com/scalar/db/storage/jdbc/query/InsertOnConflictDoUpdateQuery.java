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

public class InsertOnConflictDoUpdateQuery extends AbstractQuery implements UpsertQuery {

  private final Table table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  private final Map<String, Value> values;

  InsertOnConflictDoUpdateQuery(Builder builder) {
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  protected String sql() {
    return "INSERT INTO "
        + table
        + " "
        + makeValuesSQLString()
        + " "
        + makeOnConflictDoUpdateSQLString();
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

  private String makeOnConflictDoUpdateSQLString() {
    List<String> primaryKeys = new ArrayList<>();
    for (Value value : partitionKey) {
      primaryKeys.add(value.getName());
    }
    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
        primaryKeys.add(value.getName());
      }
    }

    return "ON CONFLICT ("
        + String.join(",", primaryKeys)
        + ") DO UPDATE SET "
        + values.keySet().stream().map(n -> n + "=?").collect(Collectors.joining(","));
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

    // For ON DUPLICATE KEY UPDATE
    for (Value value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }
}
