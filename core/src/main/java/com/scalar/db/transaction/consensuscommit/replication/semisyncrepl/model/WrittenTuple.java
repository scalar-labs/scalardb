package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = InsertedTuple.class, name = "insert"),
  @JsonSubTypes.Type(value = UpdatedTuple.class, name = "update"),
  @JsonSubTypes.Type(value = DeletedTuple.class, name = "delete"),
})
public class WrittenTuple {
  public final String namespace;
  public final String table;
  public final int txVersion;
  public final long txPreparedAtInMillis;
  public final Key partitionKey;
  public final Key clusteringKey;

  public WrittenTuple(
      String namespace,
      String table,
      int txVersion,
      long txPreparedAtInMillis,
      Key partitionKey,
      @Nullable Key clusteringKey) {
    this.namespace = namespace;
    this.table = table;
    this.txVersion = txVersion;
    this.txPreparedAtInMillis = txPreparedAtInMillis;
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", namespace)
        .add("table", table)
        .add("txVersion", txVersion)
        .add("txPreparedAtInMillis", txPreparedAtInMillis)
        .add("partitionKey", partitionKey)
        .add("clusteringKey", clusteringKey)
        .toString();
  }
}
