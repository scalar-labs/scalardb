package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

// TODO: Rename this to WriteOperation
public class Record {
  public final String namespace;
  public final String table;
  public final Key pk;
  public final Key ck;
  public final long version;
  @Nullable public final String currentTxId;
  // TODO: Rename this to `reservedTxId`?
  @Nullable public final String prepTxId;
  public final Set<Value> values;
  public final boolean deleted;
  // TODO: `deleted` is added, so we can remove this field?
  public final Set<String> insertTxIds;
  @Nullable public final Instant appendedAt;
  @Nullable public final Instant shrinkedAt;

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
      if (this == o) return true;
      if (!(o instanceof Value)) return false;
      Value value = (Value) o;
      return txVersion == value.txVersion
          && Objects.equals(prevTxId, value.prevTxId)
          && Objects.equals(txId, value.txId)
          && Objects.equals(type, value.type)
          && Objects.equals(columns, value.columns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(prevTxId, txId, txVersion, type, columns);
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

    public String toStringOnlyWithMetadata() {
      return MoreObjects.toStringHelper(this)
          .add("prevTxId", prevTxId)
          .add("txId", txId)
          .add("txVersion", txVersion)
          .add("txPreparedAtInMillis", txPreparedAtInMillis)
          .add("txCommittedAtInMillis", txCommittedAtInMillis)
          .add("type", type)
          .toString();
    }
  }

  public Record(
      String namespace,
      String table,
      Key pk,
      Key ck,
      long version,
      @Nullable String currentTxId,
      @Nullable String prepTxId,
      boolean deleted,
      Set<Value> values,
      Set<String> insertTxIds,
      @Nullable Instant appendedAt,
      @Nullable Instant shrinkedAt) {
    this.namespace = namespace;
    this.table = table;
    this.pk = pk;
    this.ck = ck;
    this.version = version;
    this.currentTxId = currentTxId;
    this.prepTxId = prepTxId;
    this.deleted = deleted;
    this.values = values;
    this.insertTxIds = insertTxIds;
    this.appendedAt = appendedAt;
    this.shrinkedAt = shrinkedAt;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", namespace)
        .add("table", table)
        .add("pk", pk)
        .add("ck", ck)
        .add("version", version)
        .add("currentTxId", currentTxId)
        .add("prepTxId", prepTxId)
        .add("deleted", deleted)
        .add("values", values)
        .add("insertTxIds", insertTxIds)
        .add("appendedAt", appendedAt)
        .add("shrinkedAt", shrinkedAt)
        .toString();
  }

  public String toStringOnlyWithMetadata() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", namespace)
        .add("table", table)
        .add("pk", pk)
        .add("ck", ck)
        .add("version", version)
        .add("currentTxId", currentTxId)
        .add("prepTxId", prepTxId)
        .add("deleted", deleted)
        .add(
            "values",
            "["
                + values.stream()
                    .map(Value::toStringOnlyWithMetadata)
                    .collect(Collectors.joining(","))
                + "]")
        .add("insertTxIds", insertTxIds)
        .add("appendedAt", appendedAt)
        .add("shrinkedAt", shrinkedAt)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Record)) {
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
