package com.scalar.db.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstraction for the selection operations such as {@link Get} and {@link Scan}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public abstract class Selection extends Operation {
  private final List<String> projections;
  private final ImmutableSet<Conjunction> conjunctions;

  Selection(
      @Nullable String namespace,
      String tableName,
      Key partitionKey,
      @Nullable Key clusteringKey,
      @Nullable Consistency consistency,
      List<String> projections,
      ImmutableSet<Conjunction> conjunctions) {
    super(namespace, tableName, partitionKey, clusteringKey, consistency);
    this.projections = projections;
    this.conjunctions = conjunctions;
  }

  /**
   * @param partitionKey a partition key
   * @param clusteringKey a clustering key
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  public Selection(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
    projections = new ArrayList<>();
    conjunctions = ImmutableSet.of();
  }

  /**
   * @param selection a selection
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  public Selection(Selection selection) {
    super(selection);
    projections = new ArrayList<>(selection.projections);
    conjunctions = selection.conjunctions;
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
   * Returns the set of {@code Conjunction}. We regard this set as a disjunction of conjunctions
   * (i.e., a disjunctive normal form, DNF).
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return set of {@code Conjunction}
   */
  @Nonnull
  public Set<Conjunction> getConjunctions() {
    return conjunctions;
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
    return projections.equals(other.projections) && conjunctions.equals(other.conjunctions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), projections, conjunctions);
  }

  /**
   * A conjunctive set of {@link ConditionalExpression}, and it is an internal representation of the
   * optional parameter for {@link Selection} operations, which specifies arbitrary conditions with
   * a disjunction of {@link Conjunction}s (i.e., a disjunctive normal form, DNF). Its functionality
   * is similar to {@link AndConditionSet}, but unlike {@link AndConditionSet}, this class is
   * primarily used for an internal purpose. Breaking changes can and will be introduced to this
   * class. Users should not depend on it.
   */
  @Immutable
  public static class Conjunction {
    private final ImmutableSet<ConditionalExpression> conditions;

    private Conjunction(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Returns the set of {@code ConditionalExpression} which this conjunction is composed of.
     *
     * @return set of {@code ConditionalExpression} which this conjunction is composed of
     */
    public Set<ConditionalExpression> getConditions() {
      return conditions;
    }

    /**
     * Creates a {@code Conjunction} object with a single conditional expression.
     *
     * @param condition a conditional expression
     * @return a {@code Conjunction} object
     */
    public static Conjunction of(ConditionalExpression condition) {
      return new Conjunction(ImmutableSet.of(condition));
    }

    /**
     * Creates a {@code Conjunction} object with a collection of conditional expressions.
     *
     * @param conditions a collection of conditional expressions
     * @return a {@code Conjunction} object
     */
    public static Conjunction of(Collection<ConditionalExpression> conditions) {
      return new Conjunction(ImmutableSet.copyOf(conditions));
    }

    /**
     * Creates a {@code Conjunction} object with conditional expressions.
     *
     * @param conditions conditional expressions
     * @return a {@code Conjunction} object
     */
    public static Conjunction of(ConditionalExpression... conditions) {
      return new Conjunction(ImmutableSet.copyOf(conditions));
    }

    /**
     * Indicates whether some other object is "equal to" this object. The other object is considered
     * equal if:
     *
     * <ul>
     *   <li>it is also an {@code Conjunction}
     *   <li>both instances have the same set of {@code ConditionalExpression}
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
      if (!(o instanceof Conjunction)) {
        return false;
      }
      Conjunction other = (Conjunction) o;
      return conditions.equals(other.conditions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(conditions);
    }

    @Override
    public String toString() {
      return conditions.toString();
    }
  }
}
