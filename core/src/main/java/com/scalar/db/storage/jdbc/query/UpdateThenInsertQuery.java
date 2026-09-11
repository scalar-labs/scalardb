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
 * An upsert written as an UPDATE followed by an INSERT that only runs when the UPDATE matched
 * nothing, as a language batch.
 *
 * <p>SAP ASE has MERGE and it works, but it compiles the WHEN NOT MATCHED THEN INSERT branch even
 * for a row that is there, and rejects the whole statement with error 233 if the insert would leave
 * a NOT NULL column without a value. A ScalarDB upsert only carries the columns being written, so
 * on any table with a NOT NULL column that is not one of them the MERGE fails even though it would
 * only ever have taken the update branch. That rules MERGE out for a table ScalarDB did not create,
 * which usually has NOT NULL columns of its own.
 *
 * <p>Splitting the two statements avoids it: ASE compiles the INSERT only when the UPDATE matched
 * nothing, so updating an existing row never touches it. Inserting a genuinely new row still fails
 * if a NOT NULL column has no value, which is correct.
 *
 * <p>The two statements go in one language batch, which is why the ASE connection sets {@code
 * DYNAMIC_PREPARE=false}: ASE cannot send a multi statement batch as one prepared statement.
 *
 * <p>It also cannot go in a JDBC batch -- ASE answers one with "Only single DML command without
 * references to local or global variables can be executed with homogeneous batch parameters" -- so
 * {@code JdbcCrudService} runs each of these as its own round trip. Promoting that decision to a
 * method on {@link Query} would be tidier than the instanceof check it uses today.
 */
@ThreadSafe
public class UpdateThenInsertQuery implements UpsertQuery {

  private final RdbEngineStrategy rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public UpdateThenInsertQuery(Builder builder) {
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
    List<String> enclosedKeyNames = new ArrayList<>();
    partitionKey.getColumns().forEach(v -> enclosedKeyNames.add(rdbEngine.enclose(v.getName())));
    clusteringKey.ifPresent(
        k -> k.getColumns().forEach(v -> enclosedKeyNames.add(rdbEngine.enclose(v.getName()))));

    List<String> enclosedValueNames =
        columns.keySet().stream().map(rdbEngine::enclose).collect(Collectors.toList());

    String fullTableName = rdbEngine.encloseFullTableName(schema, table);
    String primaryKeyConditions =
        enclosedKeyNames.stream().map(n -> n + "=?").collect(Collectors.joining(" AND "));

    StringBuilder sql = new StringBuilder();
    if (!columns.isEmpty()) {
      sql.append("UPDATE ")
          .append(fullTableName)
          .append(" SET ")
          .append(enclosedValueNames.stream().map(n -> n + "=?").collect(Collectors.joining(",")))
          .append(" WHERE ")
          .append(primaryKeyConditions)
          .append("\n");
    } else {
      // Nothing to update, so only the presence of the row decides whether to insert. This is a no
      // op UPDATE rather than a SELECT because it has to set @@rowcount without producing a result
      // set, which executeUpdate would reject with "JZ0P1: Unexpected result type".
      String firstKeyName = enclosedKeyNames.get(0);
      sql.append("UPDATE ")
          .append(fullTableName)
          .append(" SET ")
          .append(firstKeyName)
          .append("=")
          .append(firstKeyName)
          .append(" WHERE ")
          .append(primaryKeyConditions)
          .append("\n");
    }

    List<String> insertColumnNames = new ArrayList<>(enclosedKeyNames);
    insertColumnNames.addAll(enclosedValueNames);
    sql.append("IF @@rowcount = 0\n")
        .append("INSERT INTO ")
        .append(fullTableName)
        .append(" (")
        .append(String.join(",", insertColumnNames))
        .append(") VALUES (")
        .append(insertColumnNames.stream().map(n -> "?").collect(Collectors.joining(",")))
        .append(")");
    return sql.toString();
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);

    // For the UPDATE statement, or for the no op UPDATE that stands in for it
    if (!columns.isEmpty()) {
      for (Column<?> column : columns.values()) {
        column.accept(binder);
        binder.throwSQLExceptionIfOccurred();
      }
    }
    bindPrimaryKey(binder);

    // For the INSERT statement
    bindPrimaryKey(binder);
    for (Column<?> column : columns.values()) {
      column.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }
  }

  private void bindPrimaryKey(PreparedStatementBinder binder) throws SQLException {
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
  }
}
