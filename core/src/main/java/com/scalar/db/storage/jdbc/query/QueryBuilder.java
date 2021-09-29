package com.scalar.db.storage.jdbc.query;

import com.scalar.db.storage.jdbc.RdbEngine;
import java.util.List;
import java.util.Objects;

/**
 * A query builder to build SQL.
 *
 * @author Toshihiro Suzuki
 */
public final class QueryBuilder {

  private final RdbEngine rdbEngine;

  public QueryBuilder(RdbEngine rdbEngine) {
    this.rdbEngine = Objects.requireNonNull(rdbEngine);
  }

  public SelectQuery.Builder select(List<String> projections) {
    return new SelectQuery.Builder(rdbEngine, projections);
  }

  public InsertQuery.Builder insertInto(String schema, String table) {
    return new InsertQuery.Builder(rdbEngine, schema, table);
  }

  public UpdateQuery.Builder update(String schema, String table) {
    return new UpdateQuery.Builder(rdbEngine, schema, table);
  }

  public DeleteQuery.Builder deleteFrom(String schema, String table) {
    return new DeleteQuery.Builder(rdbEngine, schema, table);
  }

  public UpsertQuery.Builder upsertInto(String schema, String table) {
    return new UpsertQuery.Builder(rdbEngine, schema, table);
  }
}
