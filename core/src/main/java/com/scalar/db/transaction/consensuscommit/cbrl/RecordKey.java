package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.Key;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Identity of a single user record: {@code (namespace, table, partitionKey, clusteringKey)}. The
 * bucketing key for pass 1 and the single-owner unit for pass 2. The proto {@link Key} values carry
 * their own structural {@code equals}/{@code hashCode}, so this is a straightforward value type.
 */
final class RecordKey {
  private final String namespace;
  private final String table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;

  RecordKey(String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    this.namespace = namespace;
    this.table = table;
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }

  static RecordKey from(Entry entry) {
    return new RecordKey(
        entry.getNamespaceName(),
        entry.getTableName(),
        entry.getPartitionKey(),
        entry.hasClusteringKey() ? entry.getClusteringKey() : null);
  }

  String namespace() {
    return namespace;
  }

  String table() {
    return table;
  }

  Key partitionKey() {
    return partitionKey;
  }

  @Nullable
  Key clusteringKey() {
    return clusteringKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RecordKey)) {
      return false;
    }
    RecordKey other = (RecordKey) o;
    return namespace.equals(other.namespace)
        && table.equals(other.table)
        && partitionKey.equals(other.partitionKey)
        && Objects.equals(clusteringKey, other.clusteringKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, table, partitionKey, clusteringKey);
  }

  @Override
  public String toString() {
    return "RecordKey{"
        + namespace
        + "."
        + table
        + ", pk="
        + partitionKey
        + ", ck="
        + clusteringKey
        + '}';
  }
}
