package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

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
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class InsertOnConflictDoUpdateQuery implements UpsertQuery {

  private final RdbEngine rdbEngine;
  private final String schema;
  private final String table;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value<?>> values;

  InsertOnConflictDoUpdateQuery(Builder builder) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  @Override
  public String sql() {
    return "INSERT INTO "
        + enclosedFullTableName(schema, table, rdbEngine)
        + " "
        + makeValuesSqlString()
        + " "
        + makeOnConflictDoUpdateSqlString();
  }

  private String makeValuesSqlString() {
    List<String> names = new ArrayList<>();
    partitionKey.forEach(v -> names.add(v.getName()));
    clusteringKey.ifPresent(k -> k.forEach(v -> names.add(v.getName())));
    names.addAll(values.keySet());

    return "("
        + names.stream().map(n -> enclose(n, rdbEngine)).collect(Collectors.joining(","))
        + ") VALUES ("
        + names.stream().map(n -> "?").collect(Collectors.joining(","))
        + ")";
  }

  private String makeOnConflictDoUpdateSqlString() {
    List<String> primaryKeys = new ArrayList<>();
    partitionKey.forEach(v -> primaryKeys.add(v.getName()));
    clusteringKey.ifPresent(k -> k.forEach(v -> primaryKeys.add(v.getName())));

    StringBuilder sql = new StringBuilder();
    sql.append("ON CONFLICT (")
        .append(
            primaryKeys.stream().map(k -> enclose(k, rdbEngine)).collect(Collectors.joining(",")))
        .append(") DO ");
    if (!values.isEmpty()) {
      sql.append("UPDATE SET ")
          .append(
              values.keySet().stream()
                  .map(n -> enclose(n, rdbEngine) + "=?")
                  .collect(Collectors.joining(",")));
    } else {
      sql.append("NOTHING");
    }
    return sql.toString();
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder = new PreparedStatementBinder(preparedStatement);

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

    for (Value<?> value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    // For ON DUPLICATE KEY UPDATE
    for (Value<?> value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }
}
