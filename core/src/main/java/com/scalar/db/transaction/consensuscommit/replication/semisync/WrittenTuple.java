package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.io.Key;
import javax.annotation.Nullable;

class WrittenTuple {
  final String namespace;
  final String table;
  final Key partitionKey;
  final Key clusteringKey;

  public WrittenTuple(
      String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    this.namespace = namespace;
    this.table = table;
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }
}
