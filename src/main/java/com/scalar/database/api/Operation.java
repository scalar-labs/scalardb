package com.scalar.database.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.scalar.database.io.Key;
import com.scalar.database.storage.cassandra.OperationVisitor;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstraction for storage operations.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public abstract class Operation {
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;
  private Optional<String> namespace;
  private Optional<String> tableName;
  private Consistency consistency;

  public Operation(Key partitionKey, Key clusteringKey) {
    this.partitionKey = checkNotNull(partitionKey);
    this.clusteringKey = Optional.ofNullable(clusteringKey);
    namespace = Optional.empty();
    tableName = Optional.empty();
    consistency = Consistency.SEQUENTIAL;
  }

  /**
   * Returns the namespace for this operation
   *
   * @return an {@code Optional} with the returned namespace
   */
  @Nonnull
  public Optional<String> forNamespace() {
    return namespace;
  }

  /**
   * Returns the table name for this operation
   *
   * @return an {@code Optional} with the returned table name
   */
  @Nonnull
  public Optional<String> forTable() {
    return tableName;
  }

  /**
   * Sets the specified target namespace for this operation
   *
   * @param namespace target namespace for this operation
   * @return this object
   */
  public Operation forNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
    return this;
  }

  /**
   * Sets the specified target namespace for this operation
   *
   * @param tableName target table name for this operation
   * @return this object
   */
  public Operation forTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
    return this;
  }

  /**
   * Returns the partition key
   *
   * @return the partition {@code Key}
   */
  @Nonnull
  public Key getPartitionKey() {
    return partitionKey;
  }

  /**
   * Returns the clustering key
   *
   * @return the clustering {@code Key}
   */
  @Nonnull
  public Optional<Key> getClusteringKey() {
    return clusteringKey;
  }

  /**
   * Returns the consistency level for this operation
   *
   * @return the consistency level
   */
  public Consistency getConsistency() {
    return consistency;
  }

  /**
   * Sets the specified consistency level for this operation
   *
   * @param consistency consistency level to set
   * @return this object
   */
  public Operation withConsistency(Consistency consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code Operation} and
   *   <li>both instances have the same partition key, clustering key, namespace, table name and
   *       consistency
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Operation)) {
      return false;
    }
    Operation other = (Operation) o;
    return ComparisonChain.start()
            .compare(partitionKey, other.partitionKey)
            .compare(
                clusteringKey.orElse(null),
                other.clusteringKey.orElse(null),
                Ordering.natural().nullsFirst())
            .compare(
                namespace.orElse(null),
                other.namespace.orElse(null),
                Ordering.natural().nullsFirst())
            .compare(
                tableName.orElse(null),
                other.tableName.orElse(null),
                Ordering.natural().nullsFirst())
            .compare(consistency, other.consistency)
            .result()
        == 0;
  }

  /**
   * Access the specified visitor
   *
   * @param v a visitor object to access
   */
  public abstract void accept(OperationVisitor v);
}
