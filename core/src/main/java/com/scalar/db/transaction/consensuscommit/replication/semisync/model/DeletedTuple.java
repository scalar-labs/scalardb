package com.scalar.db.transaction.consensuscommit.replication.semisync.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.model.Key;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DeletedTuple extends WrittenTuple {
  public final String prevTxId;

  public DeletedTuple(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("table") String table,
      @JsonProperty("partitionKey") Key partitionKey,
      @JsonProperty("clusteringKey") @Nullable Key clusteringKey,
      @JsonProperty("prevTxId") String prevTxId) {
    //    super("delete", namespace, table, partitionKey, clusteringKey);
    super(namespace, table, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
  }
}
