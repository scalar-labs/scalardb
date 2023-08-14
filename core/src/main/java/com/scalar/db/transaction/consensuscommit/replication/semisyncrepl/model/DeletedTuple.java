package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DeletedTuple extends WrittenTuple {
  public final String prevTxId;

  public DeletedTuple(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("table") String table,
      @JsonProperty("txVersion") int txVersion,
      @JsonProperty("txPreparedAtInMillis") long txPreparedAtInMillis,
      @JsonProperty("partitionKey") Key partitionKey,
      @JsonProperty("clusteringKey") @Nullable Key clusteringKey,
      @JsonProperty("prevTxId") String prevTxId) {
    super(namespace, table, txVersion, txPreparedAtInMillis, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
  }
}
