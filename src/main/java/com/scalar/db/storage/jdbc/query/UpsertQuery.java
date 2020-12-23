package com.scalar.db.storage.jdbc.query;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RdbEngine;

import java.util.Map;
import java.util.Optional;

public interface UpsertQuery extends Query {

  class Builder {
    private final RdbEngine rdbEngine;
    final String fullTableName;
    Key partitionKey;
    Optional<Key> clusteringKey;
    Map<String, Value> values;

    Builder(RdbEngine rdbEngine, String fullTableName) {
      this.rdbEngine = rdbEngine;
      this.fullTableName = fullTableName;
    }

    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Value> values) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
      return this;
    }

    public UpsertQuery build() {
      switch (rdbEngine) {
        case MY_SQL:
          return new InsertOnDuplicateKeyUpdateQuery(this);
        case POSTGRE_SQL:
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
