package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.io.Key;
import javax.annotation.Nullable;

public class DeletedTuple extends WrittenTuple {
  final String prevTxId;

  DeletedTuple(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      String prevTxId) {
    super(namespace, table, partitionKey, clusteringKey);
    this.prevTxId = prevTxId;
  }
}
