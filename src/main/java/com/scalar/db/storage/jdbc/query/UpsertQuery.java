package com.scalar.db.storage.jdbc.query;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RDBType;
import com.scalar.db.storage.jdbc.Table;

import javax.annotation.Nullable;
import java.util.Map;

public interface UpsertQuery extends Query {

  class Builder {
    private final RDBType rdbType;
    final Table table;
    Key partitionKey;
    @Nullable Key clusteringKey;
    Map<String, Value> values;

    Builder(RDBType rdbType, Table table) {
      this.rdbType = rdbType;
      this.table = table;
    }

    public Builder values(
        Key partitionKey, @Nullable Key clusteringKey, Map<String, Value> values) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
      return this;
    }

    public UpsertQuery build() {
      if (table == null || partitionKey == null) {
        throw new IllegalStateException("table or partitionKey is null.");
      }

      switch (rdbType) {
        case MYSQL:
          return new InsertOnDuplicateKeyUpdateQuery(this);
        case POSTGRESQL:
          return new InsertOnConflictDoUpdateQuery(this);
        case ORACLE:
          return new MergeIntoQuery(this);
        case SQLSERVER:
          return new MergeQuery(this);
        default:
          throw new AssertionError("invalid rdb type: " + rdbType);
      }
    }
  }
}
