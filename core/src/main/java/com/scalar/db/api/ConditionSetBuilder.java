package com.scalar.db.api;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

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
    return new BuildableAndConditionSet(ImmutableSet.copyOf(conditions));
  }

  /**
   * Returns a builder object to build a {@code OrConditionSet}.
   *
   * @param conditions a set of conditional expressions to build a {@code OrConditionSet}.
   * @return a builder object
   */
  public static BuildableOrConditionSet orConditionSet(Set<ConditionalExpression> conditions) {
    return new BuildableOrConditionSet(ImmutableSet.copyOf(conditions));
  }

  @Immutable
  public static class AndOrConditionSetBuilder {

    private final ImmutableSet<ConditionalExpression> conditions;

    private AndOrConditionSetBuilder(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Adds a conditional expression for a {@code AndConditionSet}.
     *
     * @param condition a conditional expression for a {@code AndConditionSet}.
     * @return a builder object
     */
    public BuildableAndConditionSet and(ConditionalExpression condition) {
      return new BuildableAndConditionSet(
          new ImmutableSet.Builder<ConditionalExpression>()
              .addAll(conditions)
              .add(condition)
              .build());
    }

    /**
     * Adds a conditional expression for a {@code OrConditionSet}.
     *
     * @param condition a conditional expression for a {@code OrConditionSet}.
     * @return a builder object
     */
    public BuildableOrConditionSet or(ConditionalExpression condition) {
      return new BuildableOrConditionSet(
          new ImmutableSet.Builder<ConditionalExpression>()
              .addAll(conditions)
              .add(condition)
              .build());
    }
  }

  @Immutable
  public static class BuildableAndConditionSet {

    private final ImmutableSet<ConditionalExpression> conditions;

    BuildableAndConditionSet(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Adds a conditional expression for a {@code AndConditionSet}.
     *
     * @param condition a conditional expression for a {@code AndConditionSet}.
     * @return a builder object
     */
    public BuildableAndConditionSet and(ConditionalExpression condition) {
      return new BuildableAndConditionSet(
          new ImmutableSet.Builder<ConditionalExpression>()
              .addAll(conditions)
              .add(condition)
              .build());
    }

    /**
     * Builds an and-wise condition set with the specified conditional expressions.
     *
     * @return an and-wise condition set
     */
    public AndConditionSet build() {
      return new AndConditionSet(conditions);
    }
  }

  @Immutable
  public static class BuildableOrConditionSet {

    private final ImmutableSet<ConditionalExpression> conditions;

    private BuildableOrConditionSet(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Adds a conditional expression for a {@code OrConditionSet}.
     *
     * @param condition a conditional expression for a {@code OrConditionSet}.
     * @return a builder object
     */
    public BuildableOrConditionSet or(ConditionalExpression condition) {
      return new BuildableOrConditionSet(
          new ImmutableSet.Builder<ConditionalExpression>()
              .addAll(conditions)
              .add(condition)
              .build());
    }

    /**
     * Builds an or-wise condition set with the specified conditional expressions.
     *
     * @return an or-wise condition set
     */
    public OrConditionSet build() {
      return new OrConditionSet(conditions);
    }
  }
}
