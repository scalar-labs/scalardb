package com.scalar.db.api;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * An abstraction for mutation condition.
 *
 * @author Hiroyuki Yamada
 */
public interface MutationCondition {

  /**
   * Returns the list of conditions.
   *
   * @return a list of conditions
   */
  @Nonnull
  List<ConditionalExpression> getExpressions();

  /**
   * Visits the specified visitor
   *
   * @param visitor a visitor object to visit
   */
  void accept(MutationConditionVisitor visitor);
}
