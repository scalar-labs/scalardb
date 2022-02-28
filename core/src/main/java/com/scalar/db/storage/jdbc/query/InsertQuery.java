package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class InsertQuery implements Query {

  private final RdbEngine rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Optional<Value<?>>> values;

  private InsertQuery(Builder builder) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  @Override
  public String sql() {
    return "INSERT INTO "
        + enclosedFullTableName(schema, table, rdbEngine)
        + " "
        + makeValuesSqlString();
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

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);

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

    for (Entry<String, Optional<Value<?>>> entry : values.entrySet()) {
      if (entry.getValue().isPresent()) {
        entry.getValue().get().accept(binder);
      } else {
        binder.bindNullValue(entry.getKey());
      }
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {
    private final RdbEngine rdbEngine;
    private final String schema;
    private final String table;
    private final TableMetadata tableMetadata;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private Map<String, Optional<Value<?>>> values;

    Builder(RdbEngine rdbEngine, String schema, String table, TableMetadata tableMetadata) {
      this.rdbEngine = rdbEngine;
      this.schema = schema;
      this.table = table;
      this.tableMetadata = tableMetadata;
    }

    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Optional<Value<?>>> values) {
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
