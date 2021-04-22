package com.scalar.db.storage.jdbc.query;

import com.scalar.db.io.Value;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class PLSqlUpsertQuery extends AbstractQuery implements UpsertQuery {

  private final InsertQuery insertQuery;
  private final UpdateQuery updateQuery;
  private final Map<String, Value> values;

  public PLSqlUpsertQuery(Builder builder, QueryBuilder queryBuilder) {
    insertQuery =
        queryBuilder
            .insertInto(builder.schema, builder.table)
            .values(builder.partitionKey, builder.clusteringKey, builder.values)
            .build();
    updateQuery =
        queryBuilder
            .update(builder.schema, builder.table)
            .set(builder.values)
            .where(builder.partitionKey, builder.clusteringKey)
            .build();
    this.values = builder.values;
  }

  @Override
  protected String sql() {
    return "BEGIN "
        + insertQuery.sql()
        + ";EXCEPTION WHEN DUP_VAL_ON_INDEX THEN "
        + (values.isEmpty() ? "NULL" : updateQuery.sql())
        + ";END;";
  }

  @Override
  protected PreparedStatement prepare(Connection connection) throws SQLException {
    return connection.prepareCall(sql());
  }

  @Override
  protected void bind(PreparedStatementBinder preparedStatementBinder) throws SQLException {
    insertQuery.bind(preparedStatementBinder);
    if (!values.isEmpty()) {
      updateQuery.bind(preparedStatementBinder);
    }
  }
}
