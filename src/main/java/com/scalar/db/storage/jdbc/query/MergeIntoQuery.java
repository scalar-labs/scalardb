package com.scalar.db.storage.jdbc.query;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RdbEngine;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

public class MergeIntoQuery extends AbstractQuery implements UpsertQuery {

  private final RdbEngine rdbEngine;
  private final String schema;
  private final String table;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value> values;

  public MergeIntoQuery(Builder builder) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  @Override
  protected String sql() {
    List<String> enclosedKeyNames = new ArrayList<>();
    partitionKey.forEach(v -> enclosedKeyNames.add(enclose(v.getName(), rdbEngine)));
    clusteringKey.ifPresent(
        k -> k.forEach(v -> enclosedKeyNames.add(enclose(v.getName(), rdbEngine))));

    List<String> enclosedValueNames =
        values.keySet().stream().map(n -> enclose(n, rdbEngine)).collect(Collectors.toList());

    return "MERGE INTO "
        + enclosedFullTableName(schema, table, rdbEngine)
        + " t1 USING (SELECT "
        + makeUsingSelectSqlString(enclosedKeyNames)
        + " FROM DUAL) t2 ON ("
        + makePrimaryKeyConditionsSqlString(enclosedKeyNames)
        + ") WHEN MATCHED THEN UPDATE SET "
        + makeUpdateSetSqlString(enclosedValueNames)
        + " WHEN NOT MATCHED THEN INSERT "
        + makeInsertSqlString(enclosedKeyNames, enclosedValueNames);
  }

  private String makeUsingSelectSqlString(List<String> enclosedKeyNames) {
    return enclosedKeyNames.stream().map(n -> "? " + n).collect(Collectors.joining(","));
  }

  private String makePrimaryKeyConditionsSqlString(List<String> enclosedKeyNames) {
    return enclosedKeyNames.stream()
        .map(n -> "t1." + n + "=t2." + n)
        .collect(Collectors.joining(" AND "));
  }

  private String makeUpdateSetSqlString(List<String> enclosedValueNames) {
    return enclosedValueNames.stream().map(n -> n + "=?").collect(Collectors.joining(","));
  }

  private String makeInsertSqlString(
      List<String> enclosedKeyNames, List<String> enclosedValueNames) {
    List<String> names = new ArrayList<>(enclosedKeyNames);
    names.addAll(enclosedValueNames);
    return "("
        + String.join(",", names)
        + ") VALUES ("
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
