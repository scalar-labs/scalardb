package com.scalar.db.storage.jdbc.query;

import com.scalar.db.storage.jdbc.RDBType;
import com.scalar.db.storage.jdbc.Table;
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
  private final RDBType rdbType;

  public QueryBuilder(TableMetadataManager tableMetadataManager, RDBType rdbType) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.rdbType = Objects.requireNonNull(rdbType);
  }

  public SelectQuery.Builder select(List<String> projections) {
    return new SelectQuery.Builder(tableMetadataManager, rdbType, projections);
  }

  public InsertQuery.Builder insertInto(Table table) {
    return new InsertQuery.Builder(table);
  }

  public UpdateQuery.Builder update(Table table) {
    return new UpdateQuery.Builder(table);
  }

  public DeleteQuery.Builder deleteFrom(Table table) {
    return new DeleteQuery.Builder(table);
  }

  public UpsertQuery.Builder upsertInto(Table table) {
    return new UpsertQuery.Builder(rdbType, table);
  }
}
