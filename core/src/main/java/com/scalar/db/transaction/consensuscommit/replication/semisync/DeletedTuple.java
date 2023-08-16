package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import javax.annotation.Nullable;

public class DeletedTuple extends WrittenTuple {
  @JsonProperty public final String prevTxId;

  DeletedTuple(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      String prevTxId) {
    super("delete", namespace, table, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
  }
}
