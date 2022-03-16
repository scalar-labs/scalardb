package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.util.Map;
import java.util.Optional;

public interface UpsertQuery extends Query {

  class Builder {
    final RdbEngine rdbEngine;
    final String schema;
    final String table;
    final TableMetadata tableMetadata;
    Key partitionKey;
    Optional<Key> clusteringKey;
    Map<String, Column<?>> columns;

    Builder(RdbEngine rdbEngine, String schema, String table, TableMetadata tableMetadata) {
      this.rdbEngine = rdbEngine;
      this.schema = schema;
      this.table = table;
      this.tableMetadata = tableMetadata;
    }

    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Column<?>> columns) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.columns = columns;
      return this;
    }

    public UpsertQuery build() {
      switch (rdbEngine) {
        case MYSQL:
          return new InsertOnDuplicateKeyUpdateQuery(this);
        case POSTGRESQL:
          return new InsertOnConflictDoUpdateQuery(this);
        case ORACLE:
          return new MergeIntoQuery(this);
        case SQL_SERVER:
          return new MergeQuery(this);
        default:
          throw new AssertionError("invalid rdb engine: " + rdbEngine);
      }
    }
  }
}
