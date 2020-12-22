package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public interface SelectQuery extends Query {

  class Builder {
    private final TableMetadataManager tableMetadataManager;
    private final RdbEngine rdbEngine;
    final List<String> projections;
    String fullTableName;
    JdbcTableMetadata tableMetadata;
    Key partitionKey;
    Optional<Key> clusteringKey = Optional.empty();
    Optional<Key> commonClusteringKey = Optional.empty();
    Optional<Value> startValue = Optional.empty();
    boolean startInclusive;
    Optional<Value> endValue = Optional.empty();
    boolean endInclusive;
    List<Scan.Ordering> orderings = Collections.emptyList();
    private int limit;
    boolean isRangeQuery;

    Builder(
        TableMetadataManager tableMetadataManager, RdbEngine rdbEngine, List<String> projections) {
      this.tableMetadataManager = tableMetadataManager;
      this.rdbEngine = rdbEngine;
      this.projections = projections;
    }

    public Builder from(String fullTableName) {
      this.fullTableName = fullTableName;
      try {
        this.tableMetadata = tableMetadataManager.getTableMetadata(fullTableName);
      } catch (SQLException e) {
        throw new RuntimeException("An error occurred", e);
      }
      return this;
    }

    /*
     * Assumes this is called by get operations
     */
    public Builder where(Key partitionKey, Optional<Key> clusteringKey) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      return this;
    }

    /*
     * Assumes this is called by scan operations
     */
    public Builder where(
        Key partitionKey,
        Optional<Key> startClusteringKey,
        boolean startInclusive,
        Optional<Key> endClusteringKey,
        boolean endInclusive) {
      this.partitionKey = partitionKey;

      if (startClusteringKey.isPresent()) {
        commonClusteringKey =
            Optional.of(
                new Key(
                    startClusteringKey
                        .get()
                        .get()
                        .subList(0, startClusteringKey.get().size() - 1)));
      } else if (endClusteringKey.isPresent()) {
        commonClusteringKey =
            Optional.of(
                new Key(
                    endClusteringKey.get().get().subList(0, endClusteringKey.get().size() - 1)));
      }

      if (startClusteringKey.isPresent()) {
        startValue =
            Optional.of(startClusteringKey.get().get().get(startClusteringKey.get().size() - 1));
        this.startInclusive = startInclusive;
      }

      if (endClusteringKey.isPresent()) {
        endValue = Optional.of(endClusteringKey.get().get().get(endClusteringKey.get().size() - 1));
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
      if (limit > 0) {
        switch (rdbEngine) {
          case MY_SQL:
          case POSTGRE_SQL:
            return new SelectWithLimitQuery(this, limit);
          case ORACLE:
            return new SelectWithRowNumQuery(this, limit);
          case SQL_SERVER:
            return new SelectWithOffsetFetchQuery(this, limit);
          default:
            throw new AssertionError("Invalid rdb type: " + rdbEngine);
        }
      }
      return new SimpleSelectQuery(this);
    }
  }

  Result getResult(ResultSet resultSet) throws SQLException;
}
