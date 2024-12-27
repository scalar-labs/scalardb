package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class InsertQuery implements Query {

  private final RdbEngineStrategy<?, ?, ?, ?> rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;

  private InsertQuery(Builder builder) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    columns = builder.columns;
  }

  @Override
  public String sql() {
    return "INSERT INTO "
        + rdbEngine.encloseFullTableName(schema, table)
        + " "
        + makeValuesSqlString();
  }

  private String makeValuesSqlString() {
    List<String> names = new ArrayList<>();
    partitionKey.getColumns().forEach(v -> names.add(v.getName()));
    clusteringKey.ifPresent(k -> k.getColumns().forEach(v -> names.add(v.getName())));
    names.addAll(columns.keySet());
    return "("
        + names.stream().map(rdbEngine::enclose).collect(Collectors.joining(","))
        + ") VALUES ("
        + names.stream().map(n -> "?").collect(Collectors.joining(","))
        + ")";
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);

    for (Column<?> column : partitionKey.getColumns()) {
      column.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    if (clusteringKey.isPresent()) {
      for (Column<?> column : clusteringKey.get().getColumns()) {
        column.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }

    for (Column<?> column : columns.values()) {
      column.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  public static class Builder {
    private final RdbEngineStrategy<?, ?, ?, ?> rdbEngine;
    private final String schema;
    private final String table;
    private final TableMetadata tableMetadata;
    private Key partitionKey;
    private Optional<Key> clusteringKey;
    private Map<String, Column<?>> columns;

    Builder(
        RdbEngineStrategy<?, ?, ?, ?> rdbEngine,
        String schema,
        String table,
        TableMetadata tableMetadata) {
      this.rdbEngine = rdbEngine;
      this.schema = schema;
      this.table = table;
      this.tableMetadata = tableMetadata;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Column<?>> columns) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.columns = columns;
      return this;
    }

    public InsertQuery build() {
      return new InsertQuery(this);
    }
  }
}
