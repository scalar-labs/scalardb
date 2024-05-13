package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An optional parameter of {@link Update} for conditional update. {@link Update} with this
 * condition makes the operation take effect only if a specified condition is met.
 */
@Immutable
public class UpdateIf implements MutationCondition {
  private final List<ConditionalExpression> expressions;

  /**
   * Constructs a {@code UpdateIf} with the specified list of conditional expressions.
   *
   * @param expressions a list of expressions specified with {@code ConditionalExpression}s
   */
  UpdateIf(List<ConditionalExpression> expressions) {
    checkNotNull(expressions);
    this.expressions = ImmutableList.copyOf(expressions);
  }

  /**
   * Returns the immutable list of conditional expressions.
   *
   * @return an immutable list of conditional expressions
   */
  @Override
  @Nonnull
  public List<ConditionalExpression> getExpressions() {
    return expressions;
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
   *   <li>it is also an {@code UpdateIf} and
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
    if (!(o instanceof UpdateIf)) {
      return false;
    }
    UpdateIf other = (UpdateIf) o;
    return expressions.equals(other.expressions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("expressions", expressions).toString();
  }
}
