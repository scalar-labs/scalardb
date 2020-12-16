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

public class MergeQuery extends AbstractQuery implements UpsertQuery {

  private final Table table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  private final Map<String, Value> values;

  public MergeQuery(Builder builder) {
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  @Override
  protected String sql() {
    List<String> keyNames = new ArrayList<>();
    for (Value value : partitionKey) {
      keyNames.add(value.getName());
    }
    if (clusteringKey != null) {
      for (Value value : clusteringKey) {
        keyNames.add(value.getName());
      }
    }

    return "MERGE "
        + table
        + " t1 USING (SELECT "
        + makeUsingSelectSQLString(keyNames)
        + ") t2 ON ("
        + makePrimaryKeyConditionsSQLString(keyNames)
        + ") WHEN MATCHED THEN UPDATE SET "
        + makeUpdateSetSQLString()
        + " WHEN NOT MATCHED THEN INSERT "
        + makeInsertSQLString(keyNames)
        + ";";
  }

  private String makeUsingSelectSQLString(List<String> keyNames) {
    return keyNames.stream().map(n -> "? " + n).collect(Collectors.joining(","));
  }

  private String makePrimaryKeyConditionsSQLString(List<String> keyNames) {
    return keyNames.stream().map(n -> "t1." + n + "=t2." + n).collect(Collectors.joining(" AND "));
  }

  private String makeUpdateSetSQLString() {
    return values.keySet().stream().map(n -> n + "=?").collect(Collectors.joining(","));
  }

  private String makeInsertSQLString(List<String> keyNames) {
    List<String> names = new ArrayList<>(keyNames);
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

    // For the USING SELECT statement
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

    // For the UPDATE statement
    for (Value value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    // For the INSERT statement
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
