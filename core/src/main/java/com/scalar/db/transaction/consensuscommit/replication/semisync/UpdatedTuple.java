package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import java.util.List;
import javax.annotation.Nullable;

public class UpdatedTuple extends WrittenTuple {
  @JsonProperty public final List<Column<?>> columns;
  @JsonProperty public final String prevTxId;

  UpdatedTuple(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      String prevTxId,
      List<Column<?>> columns) {
    super("update", namespace, table, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
    this.columns = columns;
  }
}
