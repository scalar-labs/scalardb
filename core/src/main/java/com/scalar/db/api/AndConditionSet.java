package com.scalar.db.api;

import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

/**
 * An and-wise set of {@link ConditionalExpression} used for specifying arbitrary conditions in
 * {@link Selection} operations.
 */
@Immutable
public class AndConditionSet {
  private final ImmutableSet<ConditionalExpression> conditions;

  AndConditionSet(ImmutableSet<ConditionalExpression> conditions) {
    this.conditions = conditions;
  }

  /**
   * Returns the set of {@code ConditionalExpression}.
   *
   * @return set of {@code ConditionalExpression}
   */
  public Set<ConditionalExpression> getConditions() {
    return conditions;
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code AndConditionSet}
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
    if (!(o instanceof AndConditionSet)) {
      return false;
    }
    AndConditionSet other = (AndConditionSet) o;
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
