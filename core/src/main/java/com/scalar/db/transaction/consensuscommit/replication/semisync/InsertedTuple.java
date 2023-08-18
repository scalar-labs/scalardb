package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class InsertedTuple extends WrittenTuple {
  public final List<Column<?>> columns;

  public InsertedTuple(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("table") String table,
      @JsonProperty("partitionKey") Key partitionKey,
      @JsonProperty("clusteringKey") @Nullable Key clusteringKey,
      @JsonProperty("columns") List<Column<?>> columns) {
    //    super("insert", namespace, table, partitionKey, clusteringKey);
    super(namespace, table, partitionKey, clusteringKey);
    this.columns = columns;
  }
}
