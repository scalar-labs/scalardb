package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Optional;

public interface UpsertQuery extends Query {

  class Builder {
    final RdbEngineStrategy<?, ?, ?, ?> rdbEngine;
    final String schema;
    final String table;
    final TableMetadata tableMetadata;
    Key partitionKey;
    Optional<Key> clusteringKey;
    Map<String, Column<?>> columns;

    Builder(
        RdbEngineStrategy<?, ?, ?, ?> rdbEngine,
        String schema,
        String table,
        TableMetadata tableMetadata) {
      this.rdbEngine = rdbEngine;
      this.schema = schema;
      this.table = table;
      this.tableMetadata = tableMetadata;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder values(
        Key partitionKey, Optional<Key> clusteringKey, Map<String, Column<?>> columns) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.columns = columns;
      return this;
    }

    public UpsertQuery build() {
      return rdbEngine.buildUpsertQuery(this);
    }
  }
}
