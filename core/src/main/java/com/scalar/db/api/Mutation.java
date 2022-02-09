package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstraction for mutation operations such as {@link Put} and {@link Delete}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public abstract class Mutation extends Operation {
  private Optional<MutationCondition> condition;

  public Mutation(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
    condition = Optional.empty();
  }

  public Mutation(Mutation mutation) {
    super(mutation);
    condition = mutation.condition;
  }

  /**
   * Returns the {@link MutationCondition}
   *
   * @return {@code MutationCondition}
   */
  @Nonnull
  public Optional<MutationCondition> getCondition() {
    return condition;
  }

  /**
   * Sets the specified {@link MutationCondition}
   *
   * @param condition a {@code MutationCondition}
   * @return this object
   */
  public Mutation withCondition(MutationCondition condition) {
    this.condition = Optional.ofNullable(condition);
    return this;
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Mutation} and
   *   <li>both instances have the same condition
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
    if (!(o instanceof Mutation)) {
      return false;
    }
    Mutation other = (Mutation) o;
    return condition.equals(other.condition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), condition);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("consistency", getConsistency())
        .add("condition", condition)
        .toString();
  }
}
