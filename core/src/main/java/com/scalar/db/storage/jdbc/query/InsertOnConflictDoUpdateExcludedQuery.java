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

/**
 * An UPSERT query for Spanner PostgreSQL that uses {@code excluded.col} references in the {@code ON
 * CONFLICT DO UPDATE SET} clause instead of bind parameters.
 *
 * <p>Spanner PostgreSQL rejects the standard {@code SET col=?} bind-parameter form and requires the
 * {@code excluded} table alias: {@code SET col=excluded.col}.
 */
@ThreadSafe
public class InsertOnConflictDoUpdateExcludedQuery implements UpsertQuery {

  private final RdbEngineStrategy rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public InsertOnConflictDoUpdateExcludedQuery(Builder builder) {
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
        + makeValuesSqlString()
        + " "
        + makeOnConflictDoUpdateSqlString();
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

  private String makeOnConflictDoUpdateSqlString() {
    List<String> primaryKeys = new ArrayList<>();
    partitionKey.getColumns().forEach(v -> primaryKeys.add(v.getName()));
    clusteringKey.ifPresent(k -> k.getColumns().forEach(v -> primaryKeys.add(v.getName())));

    StringBuilder sql = new StringBuilder();
    sql.append("ON CONFLICT (")
        .append(primaryKeys.stream().map(rdbEngine::enclose).collect(Collectors.joining(",")))
        .append(") DO ");
    if (!columns.isEmpty()) {
      sql.append("UPDATE SET ")
          .append(
              columns.keySet().stream()
                  .map(n -> rdbEngine.enclose(n) + "=excluded." + rdbEngine.enclose(n))
                  .collect(Collectors.joining(",")));
    } else {
      sql.append("NOTHING");
    }
    return sql.toString();
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
}
