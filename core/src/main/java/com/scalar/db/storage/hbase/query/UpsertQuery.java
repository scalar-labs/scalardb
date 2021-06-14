package com.scalar.db.storage.hbase.query;

import static com.scalar.db.storage.hbase.query.QueryUtils.enclose;
import static com.scalar.db.storage.hbase.query.QueryUtils.enclosedFullTableName;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class UpsertQuery extends AbstractQuery {

  private final String schema;
  private final String table;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Value<?>> values;

  private UpsertQuery(Builder builder) {
    schema = builder.schema;
    table = builder.table;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    values = builder.values;
  }

  @Override
  protected String sql() {
    return "UPSERT INTO " + enclosedFullTableName(schema, table) + " " + makeValuesSqlString();
  }

  private String makeValuesSqlString() {
    List<String> names = new ArrayList<>();
    partitionKey.forEach(v -> names.add(v.getName()));
    clusteringKey.ifPresent(k -> k.forEach(v -> names.add(v.getName())));
    names.addAll(values.keySet());
    return "("
        + enclose(HASH_COLUMN_NAME)
        + ","
        + names.stream().map(QueryUtils::enclose).collect(Collectors.joining(","))
        + ") VALUES (?,"
        + names.stream().map(n -> "?").collect(Collectors.joining(","))
        + ")";
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

    for (Value<?> value : values.values()) {
      value.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {
    final String schema;
    final String table;
    Key partitionKey;
    Optional<Key> clusteringKey;
    Map<String, Value<?>> values;

    Builder(String schema, String table) {
      this.schema = schema;
      this.table = table;
    }

    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Value<?>> values) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
      return this;
    }

    public UpsertQuery build() {
      return new UpsertQuery(this);
    }
  }
}
