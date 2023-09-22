package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import javax.annotation.Nullable;

public class RecordHolder {
  public final Key key;
  @Nullable public final Record record;

  public RecordHolder(Key key, @Nullable Record record) {
    this.key = key;
    this.record = record;
  }
}
