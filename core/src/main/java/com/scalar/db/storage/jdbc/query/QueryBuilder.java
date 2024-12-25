package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;

/**
 * A query builder to build SQL.
 *
 * @author Toshihiro Suzuki
 */
public final class QueryBuilder {

  private final RdbEngineStrategy<?, ?, ?, ?> rdbEngine;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public QueryBuilder(RdbEngineStrategy<?, ?, ?, ?> rdbEngine) {
    this.rdbEngine = Objects.requireNonNull(rdbEngine);
  }

  public SelectQuery.Builder select(List<String> projections) {
    return new SelectQuery.Builder(rdbEngine, projections);
  }

  public InsertQuery.Builder insertInto(String schema, String table, TableMetadata tableMetadata) {
    return new InsertQuery.Builder(rdbEngine, schema, table, tableMetadata);
  }

  public UpdateQuery.Builder update(String schema, String table, TableMetadata tableMetadata) {
    return new UpdateQuery.Builder(rdbEngine, schema, table, tableMetadata);
  }

  public DeleteQuery.Builder deleteFrom(String schema, String table, TableMetadata tableMetadata) {
    return new DeleteQuery.Builder(rdbEngine, schema, table, tableMetadata);
  }

  public UpsertQuery.Builder upsertInto(String schema, String table, TableMetadata tableMetadata) {
    return new UpsertQuery.Builder(rdbEngine, schema, table, tableMetadata);
  }
}
