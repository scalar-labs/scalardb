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
public class MergeIntoQuery implements UpsertQuery {

  private final RdbEngineStrategy rdbEngine;
  private final String schema;
  private final String table;
  private final TableMetadata tableMetadata;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private final Map<String, Column<?>> columns;
  private final String dualTableName;

  public static MergeIntoQuery createForOracle(Builder builder) {
    return new MergeIntoQuery(builder, "DUAL");
  }

  public static MergeIntoQuery createForDb2(Builder builder) {
    return new MergeIntoQuery(builder, "SYSIBM.DUAL");
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  private MergeIntoQuery(Builder builder, String dualTableName) {
    rdbEngine = builder.rdbEngine;
    schema = builder.schema;
    table = builder.table;
    tableMetadata = builder.tableMetadata;
    partitionKey = builder.partitionKey;
    clusteringKey = builder.clusteringKey;
    columns = builder.columns;
    this.dualTableName = dualTableName;
  }

  @Override
  public String sql() {
    List<String> enclosedKeyNames = new ArrayList<>();
    partitionKey.getColumns().forEach(v -> enclosedKeyNames.add(rdbEngine.enclose(v.getName())));
    clusteringKey.ifPresent(
        k -> k.getColumns().forEach(v -> enclosedKeyNames.add(rdbEngine.enclose(v.getName()))));

    List<String> enclosedValueNames =
        columns.keySet().stream().map(rdbEngine::enclose).collect(Collectors.toList());

    StringBuilder sql = new StringBuilder();
    sql.append("MERGE INTO ")
        .append(rdbEngine.encloseFullTableName(schema, table))
        .append(" t1 USING (SELECT ")
        .append(makeUsingSelectSqlString(enclosedKeyNames))
        .append(" FROM ")
        .append(dualTableName)
        .append(") t2 ON (")
        .append(makePrimaryKeyConditionsSqlString(enclosedKeyNames))
        .append(")");
    if (!columns.isEmpty()) {
      sql.append(" WHEN MATCHED THEN UPDATE SET ")
          .append(makeUpdateSetSqlString(enclosedValueNames));
    }
    sql.append(" WHEN NOT MATCHED THEN INSERT ")
        .append(makeInsertSqlString(enclosedKeyNames, enclosedValueNames));
    return sql.toString();
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
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    PreparedStatementBinder binder =
        new PreparedStatementBinder(preparedStatement, tableMetadata, rdbEngine);

    // For the USING SELECT statement
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

    // For the UPDATE statement
    for (Column<?> column : columns.values()) {
      column.accept(binder);
      binder.throwSQLExceptionIfOccurred();
    }

    // For the INSERT statement
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
