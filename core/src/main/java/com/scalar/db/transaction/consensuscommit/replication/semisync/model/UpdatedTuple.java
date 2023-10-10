package com.scalar.db.transaction.consensuscommit.replication.semisync.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.model.Key;
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
      @JsonProperty("partitionKey") Key partitionKey,
      @JsonProperty("clusteringKey") @Nullable Key clusteringKey,
      @JsonProperty("prevTxId") String prevTxId,
      @JsonProperty("columns") List<Column<?>> columns) {
    //    super("update", namespace, table, partitionKey, clusteringKey);
    super(namespace, table, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
    this.columns = columns;
  }
}
