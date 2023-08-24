package com.scalar.db.transaction.consensuscommit.replication.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class Record {
  private final String namespace;
  private final String table;
  private final Key pk;
  private final Key ck;
  public final long version;
  private final String currentTxId;
  private final Set<Value> values;
  private final Instant appendedAt;
  private final Instant shrinkedAt;

  /** A class that represents a write-set in `records` table. See also {@link WrittenTuple}. */
  public static class Value {
    public final String prevTxId;
    public final String txId;
    public final int txVersion;
    public final long txPreparedAtInMillis;
    public final long txCommittedAtInMillis;

    // TODO: This can be an enum.
    public final String type;
    public final Collection<Column<?>> columns;

    public Value(
        @JsonProperty("prevTxId") String prevTxId,
        @JsonProperty("txId") String txId,
        @JsonProperty("txVersion") int txVersion,
        @JsonProperty("txPreparedAtInMillis") long txPreparedAtInMillis,
        @JsonProperty("txCommittedAtInMillis") long txCommittedAtInMillis,
        @JsonProperty("type") String type,
        @JsonProperty("columns") Collection<Column<?>> columns) {
      this.prevTxId = prevTxId;
      this.txId = txId;
      this.txVersion = txVersion;
      this.txPreparedAtInMillis = txPreparedAtInMillis;
      this.txCommittedAtInMillis = txCommittedAtInMillis;
      this.type = type;
      this.columns = columns;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Value value = (Value) o;
      return Objects.equals(txId, value.txId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(txId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("prevTxId", prevTxId)
          .add("txId", txId)
          .add("txVersion", txVersion)
          .add("txPreparedAtInMillis", txPreparedAtInMillis)
          .add("txCommittedAtInMillis", txCommittedAtInMillis)
          .add("type", type)
          .add("columns", columns)
          .toString();
    }
  }

  public Record(
      String namespace,
      String table,
      Key pk,
      Key ck,
      long version,
      String currentTxId,
      Set<Value> values,
      Instant appendedAt,
      Instant shrinkedAt) {
    this.namespace = namespace;
    this.table = table;
    this.pk = pk;
    this.ck = ck;
    this.version = version;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", namespace)
        .add("table", table)
        .add("pk", pk)
        .add("ck", ck)
        .add("currentTxId", currentTxId)
        .add("values", values)
        .add("appendedAt", appendedAt)
        .add("shrinkedAt", shrinkedAt)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record record = (Record) o;
    return Objects.equals(namespace, record.namespace)
        && Objects.equals(table, record.table)
        && Objects.equals(pk, record.pk)
        && Objects.equals(ck, record.ck);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, table, pk, ck);
  }
}
