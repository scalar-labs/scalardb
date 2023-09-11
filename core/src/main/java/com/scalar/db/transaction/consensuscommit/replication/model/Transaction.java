package com.scalar.db.transaction.consensuscommit.replication.model;

import java.time.Instant;
import java.util.Collection;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Transaction {
  public final int partitionId;
  public final Instant createdAt;
  public final Instant updatedAt;
  public final String transactionId;
  public final Collection<WrittenTuple> writtenTuples;

  public Transaction(
      int partitionId,
      Instant createdAt,
      Instant updatedAt,
      String transactionId,
      Collection<WrittenTuple> writtenTuples) {
    this.partitionId = partitionId;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.transactionId = transactionId;
    this.writtenTuples = writtenTuples;
  }
}
