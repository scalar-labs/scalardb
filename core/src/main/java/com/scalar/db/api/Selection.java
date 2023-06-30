package com.scalar.db.api;

import com.google.common.collect.ImmutableList;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstraction for the selection operations such as {@link Get} and {@link Scan}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public abstract class Selection extends Operation {
  private final List<String> projections;

  /**
   * @param partitionKey a partition key
   * @param clusteringKey a clustering key
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  public Selection(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
    projections = new ArrayList<>();
  }

  /**
   * @param selection a selection
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  public Selection(Selection selection) {
    super(selection);
    projections = new ArrayList<>(selection.projections);
  }

  /**
   * Appends the specified column name to the list of projections.
   *
   * @param projection a column name to project
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  public Selection withProjection(String projection) {
    projections.add(projection);
    return this;
  }

  /**
   * Appends the specified collection of the specified column names to the list of projections.
   *
   * @param projections a collection of the column names to project
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  public Selection withProjections(Collection<String> projections) {
    this.projections.addAll(projections);
    return this;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0. */
  @Deprecated
  public void clearProjections() {
    projections.clear();
  }

  @Nonnull
  public List<String> getProjections() {
    return ImmutableList.copyOf(projections);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Selection} and
   *   <li>both instances have the same projections
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
    if (!(o instanceof Selection)) {
      return false;
    }
    Selection other = (Selection) o;
    return projections.equals(other.projections);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), projections);
  }
}
