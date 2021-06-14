package com.scalar.db.storage.hbase.query;

import com.scalar.db.storage.hbase.HBaseTableMetadataManager;
import com.scalar.db.storage.jdbc.JdbcTableMetadataManager;
import java.util.List;
import java.util.Objects;

/**
 * A query builder to build SQL.
 *
 * @author Toshihiro Suzuki
 */
public final class QueryBuilder {

  private final HBaseTableMetadataManager tableMetadataManager;

  public QueryBuilder(HBaseTableMetadataManager tableMetadataManager) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
  }

  public SelectQuery.Builder select(List<String> projections) {
    return new SelectQuery.Builder(tableMetadataManager, projections);
  }

  public UpsertQuery.Builder upsertInto(String schema, String table) {
    return new UpsertQuery.Builder(schema, table);
  }

  public DeleteQuery.Builder deleteFrom(String schema, String table) {
    return new DeleteQuery.Builder(schema, table);
  }
}
