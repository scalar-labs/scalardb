package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ComparisonChain;
import com.scalar.db.io.Key;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A multidimensional map abstraction for storage operations.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public abstract class Operation extends BaseOperation {
  private final Key partitionKey;
  private final Optional<Key> clusteringKey;

  public Operation(Key partitionKey, Key clusteringKey) {
    this.partitionKey = checkNotNull(partitionKey);
    this.clusteringKey = Optional.ofNullable(clusteringKey);
  }

  public Operation(Operation operation) {
    super(operation);
    this.partitionKey = operation.partitionKey;
    this.clusteringKey = operation.clusteringKey;
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
  @Override
  public Consistency getConsistency() {
    return super.getConsistency();
  }

  /**
   * Sets the specified consistency level for this operation
   *
   * @param consistency consistency level to set
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  @Override
  public Operation withConsistency(Consistency consistency) {
    return (Operation) super.withConsistency(consistency);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Operation} and
   *   <li>both instances have the same partition key and clustering key
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
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
                Comparator.nullsFirst(Comparator.naturalOrder()))
            .result()
        == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionKey, clusteringKey);
  }

  /**
   * Access the specified visitor
   *
   * @param v a visitor object to access
   */
  public abstract void accept(OperationVisitor v);
}
