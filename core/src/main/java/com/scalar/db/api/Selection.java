package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

  public Selection(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
    projections = new ArrayList<>();
  }

  public Selection(Selection selection) {
    super(selection);
    projections = new ArrayList<>(selection.projections);
  }

  /**
   * Appends the specified name of {@link Value} to the list of projections.
   *
   * @param projection a name of {@code Value} to project
   * @return this object
   */
  public Selection withProjection(String projection) {
    projections.add(projection);
    return this;
  }

  /**
   * Appends the specified collection of the specified names of {@code Value}s to the list of
   * projections.
   *
   * @param projections a collection of the name of {@code Value}s to project
   * @return this object
   */
  public Selection withProjections(Collection<String> projections) {
    this.projections.addAll(projections);
    return this;
  }

  public void clearProjections() {
    projections.clear();
  }

  @Nonnull
  public List<String> getProjections() {
    // TODO: use guava immutable.of
    return Collections.unmodifiableList(projections);
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
