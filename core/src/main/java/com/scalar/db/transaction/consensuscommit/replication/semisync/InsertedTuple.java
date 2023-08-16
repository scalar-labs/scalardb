package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import javax.annotation.Nullable;

public class InsertedTuple extends WrittenTuple {
  final List<Column<?>> columns;

  InsertedTuple(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      List<Column<?>> columns) {
    super(namespace, table, partitionKey, clusteringKey);
    this.columns = columns;
  }
}
