package com.scalar.db.api;

import com.scalar.db.io.Value;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An optional parameter of {@link Delete} for conditional delete. {@link Delete} with this
 * condition makes the operation take effect only if a specified entry exists.
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public class DeleteIfExists implements MutationCondition {

  public DeleteIfExists() {}

  /**
   * Returns an empty list of {@link Value}s since any conditions are not given to this object.
   *
   * @return an empty list of {@code Value}s
   */
  @Override
  @Nonnull
  public List<ConditionalExpression> getExpressions() {
    return Collections.emptyList();
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
   *   <li>it is also an {@code DeleteIfExists}
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
    return o instanceof DeleteIfExists;
  }

  @Override
  public int hashCode() {
    return DeleteIfExists.class.hashCode();
  }
}
