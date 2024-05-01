package com.scalar.db.api;

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;

public class ConditionSetBuilder {

  /**
   * Returns a builder object to build a {@code AndConditionSet} or {@code OrConditionSet}.
   *
   * @param condition a conditional expression to build a {@code AndConditionSet} or {@code
   *     OrConditionSet}.
   * @return a builder object
   */
  public static AndOrConditionSetBuilder condition(ConditionalExpression condition) {
    return new AndOrConditionSetBuilder(ImmutableSet.of(condition));
  }

  /**
   * Returns a builder object to build a {@code AndConditionSet}.
   *
   * @param conditions a set of conditional expressions to build a {@code AndConditionSet}.
   * @return a builder object
   */
  public static BuildableAndConditionSet andConditionSet(Set<ConditionalExpression> conditions) {
    return new BuildableAndConditionSet(conditions);
  }

  /**
   * Returns a builder object to build a {@code OrConditionSet}.
   *
   * @param conditions a set of conditional expressions to build a {@code OrConditionSet}.
   * @return a builder object
   */
  public static BuildableOrConditionSet orConditionSet(Set<ConditionalExpression> conditions) {
    return new BuildableOrConditionSet(conditions);
  }

  public static class AndOrConditionSetBuilder {

    private final Set<ConditionalExpression> conditions;

    AndOrConditionSetBuilder(Set<ConditionalExpression> conditions) {
      this.conditions = new HashSet<>();
      this.conditions.addAll(conditions);
    }

    /**
     * Adds a conditional expression for a {@code AndConditionSet}.
     *
     * @param condition a conditional expression for a {@code AndConditionSet}.
     * @return a builder object
     */
    public BuildableAndConditionSet and(ConditionalExpression condition) {
      conditions.add(condition);
      return new BuildableAndConditionSet(conditions);
    }

    /**
     * Adds a conditional expression for a {@code OrConditionSet}.
     *
     * @param condition a conditional expression for a {@code OrConditionSet}.
     * @return a builder object
     */
    public BuildableOrConditionSet or(ConditionalExpression condition) {
      conditions.add(condition);
      return new BuildableOrConditionSet(conditions);
    }
  }

  public static class BuildableAndConditionSet {

    private final Set<ConditionalExpression> conditions;

    BuildableAndConditionSet(Set<ConditionalExpression> conditions) {
      this.conditions = new HashSet<>();
      this.conditions.addAll(conditions);
    }

    /**
     * Adds a conditional expression for a {@code AndConditionSet}.
     *
     * @param condition a conditional expression for a {@code AndConditionSet}.
     * @return a builder object
     */
    public BuildableAndConditionSet and(ConditionalExpression condition) {
      conditions.add(condition);
      return this;
    }

    /**
     * Builds an and-wise condition set with the specified conditional expressions.
     *
     * @return an and-wise condition set
     */
    public AndConditionSet build() {
      return new AndConditionSet(ImmutableSet.copyOf(conditions));
    }
  }

  public static class BuildableOrConditionSet {

    private final Set<ConditionalExpression> conditions;

    BuildableOrConditionSet(Set<ConditionalExpression> conditions) {
      this.conditions = new HashSet<>();
      this.conditions.addAll(conditions);
    }

    /**
     * Adds a conditional expression for a {@code OrConditionSet}.
     *
     * @param condition a conditional expression for a {@code OrConditionSet}.
     * @return a builder object
     */
    public BuildableOrConditionSet or(ConditionalExpression condition) {
      conditions.add(condition);
      return this;
    }

    /**
     * Builds an or-wise condition set with the specified conditional expressions.
     *
     * @return an or-wise condition set
     */
    public OrConditionSet build() {
      return new OrConditionSet(ImmutableSet.copyOf(conditions));
    }
  }
}
