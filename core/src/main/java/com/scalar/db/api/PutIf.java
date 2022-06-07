package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An optional parameter of {@link Put} for conditional put. {@link Put} with this condition makes
 * the operation take effect only if a specified condition is met.
 */
@Immutable
public class PutIf implements MutationCondition {
  private final List<ConditionalExpression> expressions;

  /**
   * Constructs a {@code PutIf} with the specified conditional expressions.
   *
   * @param expressions a variable length expressions specified with {@code ConditionalExpression}s
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public PutIf(ConditionalExpression... expressions) {
    checkNotNull(expressions);
    this.expressions = new ArrayList<>(expressions.length);
    this.expressions.addAll(Arrays.asList(expressions));
  }

  /**
   * Constructs a {@code PutIf} with the specified list of conditional expressions.
   *
   * @param expressions a list of expressions specified with {@code ConditionalExpression}s
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public PutIf(List<ConditionalExpression> expressions) {
    checkNotNull(expressions);
    this.expressions = new ArrayList<>(expressions.size());
    this.expressions.addAll(expressions);
  }

  /**
   * Returns the immutable list of conditional expressions.
   *
   * @return an immutable list of conditional expressions
   */
  @Override
  @Nonnull
  public List<ConditionalExpression> getExpressions() {
    return ImmutableList.copyOf(expressions);
  }

  @Override
  public void accept(MutationConditionVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressions);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code PutIf} and
   *   <li>both instances have the same expressions
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
    if (!(o instanceof PutIf)) {
      return false;
    }
    PutIf other = (PutIf) o;
    return expressions.equals(other.expressions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("expressions", expressions).toString();
  }
}
