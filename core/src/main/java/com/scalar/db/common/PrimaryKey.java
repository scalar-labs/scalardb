package com.scalar.db.common;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

public class PrimaryKey implements Comparable<PrimaryKey> {

  private final String namespaceName;
  private final String tableName;
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;

  public PrimaryKey(Get get) {
    this((Operation) get);
  }

  public PrimaryKey(Put put) {
    this((Operation) put);
  }

  public PrimaryKey(Delete delete) {
    this((Operation) delete);
  }

  public PrimaryKey(Scan scan, Result result) {
    this.namespaceName = scan.forNamespace().get();
    this.tableName = scan.forTable().get();
    this.partitionKey = result.getPartitionKey().get();
    this.clusteringKey = result.getClusteringKey();
  }

  private PrimaryKey(Operation operation) {
    namespaceName = operation.forNamespace().get();
    tableName = operation.forTable().get();
    partitionKey = operation.getPartitionKey();
    clusteringKey = operation.getClusteringKey();
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public Key getPartitionKey() {
    return partitionKey;
  }

  public Optional<Key> getClusteringKey() {
    return clusteringKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, partitionKey, clusteringKey);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof PrimaryKey)) {
      return false;
    }
    PrimaryKey another = (PrimaryKey) o;
    return this.namespaceName.equals(another.namespaceName)
        && this.tableName.equals(another.tableName)
        && this.partitionKey.equals(another.partitionKey)
        && this.clusteringKey.equals(another.clusteringKey);
  }

  @Override
  public int compareTo(PrimaryKey o) {
    return ComparisonChain.start()
        .compare(this.namespaceName, o.namespaceName)
        .compare(this.tableName, o.tableName)
        .compare(this.partitionKey, o.partitionKey)
        .compare(
            this.clusteringKey.orElse(null),
            o.clusteringKey.orElse(null),
            Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("partitionKey", partitionKey)
        .add("clusteringKey", clusteringKey)
        .toString();
  }
}
