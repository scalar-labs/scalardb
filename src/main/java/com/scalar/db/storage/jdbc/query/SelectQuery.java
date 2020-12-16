package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RDBType;
import com.scalar.db.storage.jdbc.Table;
import com.scalar.db.storage.jdbc.metadata.TableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public interface SelectQuery extends Query {

  class Builder {
    private final TableMetadataManager tableMetadataManager;
    private final RDBType rdbType;
    final List<String> projections;
    Table table;
    TableMetadata tableMetadata;
    Key partitionKey;
    @Nullable Key clusteringKey;
    @Nullable Key commonClusteringKey;
    @Nullable Value startValue;
    boolean startInclusive;
    @Nullable Value endValue;
    boolean endInclusive;
    List<Scan.Ordering> orderings = Collections.emptyList();
    private int limit;
    boolean isRangeQuery;

    Builder(TableMetadataManager tableMetadataManager, RDBType rdbType, List<String> projections) {
      this.tableMetadataManager = tableMetadataManager;
      this.rdbType = rdbType;
      this.projections = projections;
    }

    public Builder from(Table table) {
      this.table = table;
      try {
        this.tableMetadata = tableMetadataManager.getTableMetadata(table);
      } catch (SQLException e) {
        throw new RuntimeException("An error occurred", e);
      }
      return this;
    }

    /*
     * Assumes this is called by get operations
     */
    public Builder where(Key partitionKey, @Nullable Key clusteringKey) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      return this;
    }

    /*
     * Assumes this is called by scan operations
     */
    public Builder where(
        Key partitionKey,
        @Nullable Key startClusteringKey,
        boolean startInclusive,
        @Nullable Key endClusteringKey,
        boolean endInclusive) {
      this.partitionKey = partitionKey;

      if (startClusteringKey != null) {
        commonClusteringKey =
            new Key(startClusteringKey.get().subList(0, startClusteringKey.size() - 1));
      } else if (endClusteringKey != null) {
        commonClusteringKey =
            new Key(endClusteringKey.get().subList(0, endClusteringKey.size() - 1));
      }

      if (startClusteringKey != null) {
        startValue = startClusteringKey.get().get(startClusteringKey.size() - 1);
        this.startInclusive = startInclusive;
      }

      if (endClusteringKey != null) {
        endValue = endClusteringKey.get().get(endClusteringKey.size() - 1);
        this.endInclusive = endInclusive;
      }

      isRangeQuery = true;

      return this;
    }

    public Builder orderBy(List<Scan.Ordering> orderings) {
      this.orderings = orderings;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public SelectQuery build() {
      if (table == null || partitionKey == null) {
        throw new IllegalStateException("table or partitionKey is null.");
      }

      if (limit > 0) {
        switch (rdbType) {
          case MYSQL:
          case POSTGRESQL:
            return new SelectWithLimitQuery(this, limit);
          case ORACLE:
            return new SelectWithRowNumQuery(this, limit);
          case SQLSERVER:
            return new SelectWithOffsetFetchQuery(this, limit);
          default:
            throw new AssertionError("Invalid rdb type: " + rdbType);
        }
      }
      return new SimpleSelectQuery(this);
    }
  }

  Result getResult(ResultSet resultSet) throws SQLException;
}
