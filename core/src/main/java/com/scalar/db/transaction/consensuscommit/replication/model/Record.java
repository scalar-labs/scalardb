package com.scalar.db.transaction.consensuscommit.replication.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;

public class Record {
  private final String namespace;
  private final String table;
  private final Key pk;
  private final Key ck;
  private final String currentTxId;
  private final Set<Value> values;
  private final Instant appendedAt;
  private final Instant shrinkedAt;

  /** A class that represents a write-set in `records` table. See also {@link WrittenTuple}. */
  public static class Value {
    public final String prevTxId;
    public final String txId;
    // TODO: This can be an enum.
    public final String type;
    public final Collection<Column<?>> columns;

    public Value(
        @JsonProperty("prevTxId") String prevTxId,
        @JsonProperty("txId") String txId,
        @JsonProperty("type") String type,
        @JsonProperty("columns") Collection<Column<?>> columns) {
      this.prevTxId = prevTxId;
      this.txId = txId;
      this.type = type;
      this.columns = columns;
    }
  }

  public Record(
      String namespace,
      String table,
      Key pk,
      Key ck,
      String currentTxId,
      Set<Value> values,
      Instant appendedAt,
      Instant shrinkedAt) {
    this.namespace = namespace;
    this.table = table;
    this.pk = pk;
    this.ck = ck;
    this.currentTxId = currentTxId;
    this.values = values;
    this.appendedAt = appendedAt;
    this.shrinkedAt = shrinkedAt;
  }

  public String namespace() {
    return namespace;
  }

  public String table() {
    return table;
  }

  public Key pk() {
    return pk;
  }

  public Key ck() {
    return ck;
  }

  public String currentTxId() {
    return currentTxId;
  }

  public Set<Value> values() {
    return values;
  }

  public Instant appendedAt() {
    return appendedAt;
  }

  public Instant shrinkedAt() {
    return shrinkedAt;
  }
}
