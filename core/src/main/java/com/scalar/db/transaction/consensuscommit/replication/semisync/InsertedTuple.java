package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import java.util.List;
import javax.annotation.Nullable;

public class InsertedTuple extends WrittenTuple {
  @JsonProperty public final List<Column<?>> columns;

  InsertedTuple(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      List<Column<?>> columns) {
    super("insert", namespace, table, partitionKey, clusteringKey);
    this.columns = columns;
  }
}
