package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.google.common.base.MoreObjects;
import java.time.Instant;

public class UpdatedRecord {
  public final int partitionId;
  public final String namespace;
  public final String table;
  public final Key pk;
  public final Key ck;
  public final Instant updatedAt;
  // TODO: This isn't used as a total order. Use tx_id instead since it's more robust.
  public final long version;

  public UpdatedRecord(
      int partitionId,
      String namespace,
      String table,
      Key pk,
      Key ck,
      Instant updatedAt,
      long version) {
    this.partitionId = partitionId;
    this.namespace = namespace;
    this.table = table;
    this.pk = pk;
    this.ck = ck;
    this.updatedAt = updatedAt;
    this.version = version;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitionId", partitionId)
        .add("namespace", namespace)
        .add("table", table)
        .add("pk", pk)
        .add("ck", ck)
        .add("updatedAt", updatedAt)
        .add("version", version)
        .toString();
  }
}
