package com.scalar.database.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An optional parameter of {@link Delete} for conditional delete. {@link Delete} with this
 * condition makes the operation take effect only if a specified condition is met.
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public class DeleteIf implements MutationCondition {
  private final List<ConditionalExpression> expressions;

  /**
   * Constructs a {@code DeleteIf} with the specified conditional expressions.
   *
   * @param expressions a variable length expressions specified with {@code ConditionalExpression}s
   */
  public DeleteIf(ConditionalExpression... expressions) {
    checkNotNull(expressions);
    this.expressions = new ArrayList<>(expressions.length);
    Arrays.stream(expressions).forEach(v -> this.expressions.add(v));
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

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code DeleteIf} and
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
    if (!(o instanceof DeleteIf)) {
      return false;
    }
    DeleteIf other = (DeleteIf) o;
    return expressions.equals(other.expressions);
  }
}
