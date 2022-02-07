package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import java.util.Collection;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve an entry from {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Get extends Selection {

  /**
   * Constructs a {@code Get} with the specified partition {@code Key}.
   *
   * @param partitionKey a partition key (it might be composed of multiple values)
   */
  public Get(Key partitionKey) {
    this(partitionKey, null);
  }

  /**
   * Constructs a {@code Get} with the specified partition {@link Key} and the clustering {@link
   * Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
   */
  public Get(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
  }

  public Get(Get get) {
    super(get);
  }

  @Override
  public Get forNamespace(String namespace) {
    return (Get) super.forNamespace(namespace);
  }

  @Override
  public Get forTable(String tableName) {
    return (Get) super.forTable(tableName);
  }

  @Override
  public Get withConsistency(Consistency consistency) {
    return (Get) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  @Override
  public Get withProjection(String projection) {
    return (Get) super.withProjection(projection);
  }

  @Override
  public Get withProjections(Collection<String> projections) {
    return (Get) super.withProjections(projections);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Get}
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
    return o instanceof Get;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("projections", getProjections())
        .add("consistency", getConsistency())
        .toString();
  }
}
