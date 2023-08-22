package com.scalar.db.transaction.consensuscommit.replication.model;

import java.time.Instant;
import java.util.Collection;

public class Transaction {
  private final int partitionId;
  private final Instant createdAt;
  private final String transactionId;
  private final Collection<WrittenTuple> writtenTuples;

  public Transaction(
      int partitionId,
      Instant createdAt,
      String transactionId,
      Collection<WrittenTuple> writtenTuples) {
    this.partitionId = partitionId;
    this.createdAt = createdAt;
    this.transactionId = transactionId;
    this.writtenTuples = writtenTuples;
  }

  public int partitionId() {
    return partitionId;
  }

  public Instant createdAt() {
    return createdAt;
  }

  public String transactionId() {
    return transactionId;
  }

  public Collection<WrittenTuple> writtenTuples() {
    return writtenTuples;
  }
}
