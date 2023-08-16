package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import javax.annotation.Nullable;

public class UpdatedTuple extends WrittenTuple {
  final List<Column<?>> columns;
  final String prevTxId;

  UpdatedTuple(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      String prevTxId,
      List<Column<?>> columns) {
    super(namespace, table, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
    this.columns = columns;
  }
}
