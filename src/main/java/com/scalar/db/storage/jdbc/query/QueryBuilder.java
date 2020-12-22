package com.scalar.db.storage.jdbc.query;

import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.List;
import java.util.Objects;

/**
 * A query builder to build SQL
 *
 * @author Toshihiro Suzuki
 */
public final class QueryBuilder {

  private final TableMetadataManager tableMetadataManager;
  private final RdbEngine rdbEngine;

  public QueryBuilder(TableMetadataManager tableMetadataManager, RdbEngine rdbEngine) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.rdbEngine = Objects.requireNonNull(rdbEngine);
  }

  public SelectQuery.Builder select(List<String> projections) {
    return new SelectQuery.Builder(tableMetadataManager, rdbEngine, projections);
  }

  public InsertQuery.Builder insertInto(String fullTableName) {
    return new InsertQuery.Builder(fullTableName);
  }

  public UpdateQuery.Builder update(String fullTableName) {
    return new UpdateQuery.Builder(fullTableName);
  }

  public DeleteQuery.Builder deleteFrom(String fullTableName) {
    return new DeleteQuery.Builder(fullTableName);
  }

  public UpsertQuery.Builder upsertInto(String fullTableName) {
    return new UpsertQuery.Builder(rdbEngine, fullTableName);
  }
}
