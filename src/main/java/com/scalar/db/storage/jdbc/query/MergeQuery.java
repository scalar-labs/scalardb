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

public class MergeQuery extends AbstractQuery implements UpsertQuery {

  private final String fullTableName;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value> values;

  public MergeQuery(Builder builder) {
    fullTableName = builder.fullTableName;
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

    clusteringKey.ifPresent(ckey -> ckey.forEach(v -> keyNames.add(v.getName())));

    return "MERGE "
        + fullTableName
        + " t1 USING (SELECT "
        + makeUsingSelectSqlString(keyNames)
        + ") t2 ON ("
        + makePrimaryKeyConditionsSqlString(keyNames)
        + ") WHEN MATCHED THEN UPDATE SET "
        + makeUpdateSetSqlString()
        + " WHEN NOT MATCHED THEN INSERT "
        + makeInsertSqlString(keyNames)
        + ";";
  }

  private String makeUsingSelectSqlString(List<String> keyNames) {
    return keyNames.stream().map(n -> "? " + n).collect(Collectors.joining(","));
  }

  private String makePrimaryKeyConditionsSqlString(List<String> keyNames) {
    return keyNames.stream().map(n -> "t1." + n + "=t2." + n).collect(Collectors.joining(" AND "));
  }

  private String makeUpdateSetSqlString() {
    return values.keySet().stream().map(n -> n + "=?").collect(Collectors.joining(","));
  }

  private String makeInsertSqlString(List<String> keyNames) {
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
    if (clusteringKey.isPresent()) {
      for (Value value : clusteringKey.get()) {
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
}
