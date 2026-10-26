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
public class InsertOnDuplicateKeyUpdateQuery implements UpsertQuery {

  private final RdbEngineStrategy rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;
  private final boolean useRowAlias;

  /**
   * @param useRowAlias whether the update reads the inserted row through a row alias ({@code VALUES
   *     (...) AS new ... col=new.col}) or through {@code VALUES(col)}, which MySQL 8.0.20
   *     deprecates
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public InsertOnDuplicateKeyUpdateQuery(Builder builder, boolean useRowAlias) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    columns = builder.columns;
    this.useRowAlias = useRowAlias;
  }

  @Override
  public String sql() {
    StringBuilder sql;
    if (!columns.isEmpty()) {
      sql = new StringBuilder("INSERT INTO ");
    } else {
      sql = new StringBuilder("INSERT IGNORE INTO ");
    }
    sql.append(rdbEngine.encloseFullTableName(schema, table))
        .append(" ")
        .append(makeValuesSqlString());
    if (!columns.isEmpty()) {
      if (useRowAlias) {
        sql.append(" AS ").append(rdbEngine.enclose(rowAlias()));
      }
      sql.append(" ").append(makeOnDuplicateKeyUpdateSqlString());
    }
    return sql.toString();
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

  // MySQL rejects a row alias equal to the table name.
  private String rowAlias() {
    return table.equalsIgnoreCase("new") ? "new_row" : "new";
  }

  private String makeOnDuplicateKeyUpdateSqlString() {
    String insertedRow = useRowAlias ? rdbEngine.enclose(rowAlias()) : null;
    return "ON DUPLICATE KEY UPDATE "
        + columns.keySet().stream()
            .map(rdbEngine::enclose)
            .map(n -> n + "=" + (useRowAlias ? insertedRow + "." + n : "VALUES(" + n + ")"))
            .collect(Collectors.joining(","));
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
