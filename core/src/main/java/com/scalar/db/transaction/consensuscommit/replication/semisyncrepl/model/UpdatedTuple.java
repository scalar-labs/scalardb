package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class UpdatedTuple extends WrittenTuple {
  public final List<Column<?>> columns;
  public final String prevTxId;

  public UpdatedTuple(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("table") String table,
      @JsonProperty("txVersion") int txVersion,
      @JsonProperty("txPreparedAtInMillis") long txPreparedAtInMillis,
      @JsonProperty("partitionKey") Key partitionKey,
      @JsonProperty("clusteringKey") @Nullable Key clusteringKey,
      @JsonProperty("prevTxId") String prevTxId,
      @JsonProperty("columns") List<Column<?>> columns) {
    super(namespace, table, txVersion, txPreparedAtInMillis, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
    this.columns = columns;
  }
}
