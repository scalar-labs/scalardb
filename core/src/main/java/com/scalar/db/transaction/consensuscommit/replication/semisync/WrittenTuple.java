package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import javax.annotation.Nullable;

class WrittenTuple {
  @JsonProperty public final String type;
  @JsonProperty public final String namespace;
  @JsonProperty public final String table;
  @JsonProperty public final Key partitionKey;
  @JsonProperty public final Key clusteringKey;

  public WrittenTuple(
      String type, String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    this.type = type;
    this.namespace = namespace;
    this.table = table;
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }
}
