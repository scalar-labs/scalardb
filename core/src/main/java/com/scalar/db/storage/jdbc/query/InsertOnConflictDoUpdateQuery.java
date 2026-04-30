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
 * An UPSERT query using {@code INSERT ... ON CONFLICT (pk) DO UPDATE SET ...}.
 *
 * <p>The SET-clause format depends on the backend. Standard PostgreSQL and SQLite accept bind
 * parameters ({@code SET col=?}), while Spanner PostgreSQL rejects that form and requires the
 * {@code excluded} table alias ({@code SET col=excluded.col}). Pass {@code useExcludedAlias=true}
 * to produce the Spanner-compatible form, which also skips the second bind pass since the SET
 * clause no longer contains placeholders.
 */
@ThreadSafe
public class InsertOnConflictDoUpdateQuery implements UpsertQuery {

  private final RdbEngineStrategy rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;
  private final boolean useExcludedAlias;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public InsertOnConflictDoUpdateQuery(Builder builder) {
    this(builder, false);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public InsertOnConflictDoUpdateQuery(Builder builder, boolean useExcludedAlias) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    columns = builder.columns;
    this.useExcludedAlias = useExcludedAlias;
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
                  .map(
                      n ->
                          useExcludedAlias
                              ? rdbEngine.enclose(n) + "=excluded." + rdbEngine.enclose(n)
                              : rdbEngine.enclose(n) + "=?")
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

    if (!useExcludedAlias) {
      // Second bind pass for the "SET col=?" placeholders in the ON CONFLICT DO UPDATE clause.
      // When useExcludedAlias is true, the SET clause uses excluded.col references with no
      // placeholders, so this pass is skipped.
      for (Column<?> column : columns.values()) {
        column.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }
  }
}
