package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.util.Collection;
import javax.annotation.concurrent.Immutable;

@Immutable
// TODO: Rename this to WriteSet
public class Transaction {
  public final int partitionId;
  public final Instant createdAt;
  public final Instant updatedAt;
  public final String transactionId;
  public final Collection<WrittenTuple> writtenTuples;
  // FIXME
  @JsonIgnore public final String type = null;

  public Transaction(
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("createdAt") Instant createdAt,
      @JsonProperty("updatedAt") Instant updatedAt,
      @JsonProperty("transactionId") String transactionId,
      @JsonProperty("writtenTuples") Collection<WrittenTuple> writtenTuples) {
    this.partitionId = partitionId;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.transactionId = transactionId;
    this.writtenTuples = writtenTuples;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitionId", partitionId)
        .add("createdAt", createdAt)
        .add("updatedAt", updatedAt)
        .add("transactionId", transactionId)
        .add("writtenTuples", writtenTuples)
        .add("type", type)
        .toString();
  }
}
