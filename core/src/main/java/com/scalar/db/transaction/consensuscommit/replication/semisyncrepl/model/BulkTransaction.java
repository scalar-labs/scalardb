package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import java.time.Instant;
import java.util.Collection;
import javax.annotation.concurrent.Immutable;

@Immutable
public class BulkTransaction {
  public final int partitionId;
  public final Instant updatedAt;
  public final String uniqueId;
  public final Collection<Transaction> transactions;

  public BulkTransaction(
      int partitionId, Instant updatedAt, String uniqueId, Collection<Transaction> transactions) {
    this.partitionId = partitionId;
    this.updatedAt = updatedAt;
    this.uniqueId = uniqueId;
    this.transactions = transactions;
  }
}
