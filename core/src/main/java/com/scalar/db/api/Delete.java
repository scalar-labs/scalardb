package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to delete an entry from {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Delete extends Mutation {

  /**
   * Constructs a {@code Delete} with the specified partition {@code Key}.
   *
   * @param partitionKey a partition key (it might be composed of multiple values)
   */
  public Delete(Key partitionKey) {
    this(partitionKey, null);
  }

  /**
   * Constructs a {@code Delete} with the specified partition {@link Key} and the clustering {@link
   * Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
   */
  public Delete(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
  }

  public Delete(Delete delete) {
    super(delete);
  }

  @Override
  public Delete forNamespace(String namespace) {
    return (Delete) super.forNamespace(namespace);
  }

  @Override
  public Delete forTable(String tableName) {
    return (Delete) super.forTable(tableName);
  }

  @Override
  public Delete withConsistency(Consistency consistency) {
    return (Delete) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  @Override
  public Delete withCondition(MutationCondition condition) {
    return (Delete) super.withCondition(condition);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Delete}
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
    return o instanceof Delete;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("consistency", getConsistency())
        .add("condition", getCondition())
        .toString();
  }
}
