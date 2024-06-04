package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.scalar.db.api.Selection.Conjunction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

abstract class SelectionBuilder {

  static class Where {

    @Nullable ConditionalExpression condition;
    final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();
    final Set<Set<ConditionalExpression>> disjunctions = new HashSet<>();

    Where(ConditionalExpression condition) {
      this.condition = condition;
    }

    void and(ConditionalExpression condition) {
      checkNotNull(condition);
      if (this.condition != null) {
        disjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      disjunctions.add(ImmutableSet.of(condition));
    }

    void and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      if (this.condition != null) {
        disjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      disjunctions.add(orConditionSet.getConditions());
    }

    void and(Set<OrConditionSet> orConditionSets) {
      disjunctions.addAll(
          orConditionSets.stream().map(OrConditionSet::getConditions).collect(Collectors.toSet()));
    }

    void or(ConditionalExpression condition) {
      if (this.condition != null) {
        conjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      conjunctions.add(ImmutableSet.of(condition));
    }

    void or(AndConditionSet andConditionSet) {
      if (this.condition != null) {
        conjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      conjunctions.add(andConditionSet.getConditions());
    }

    void or(Set<AndConditionSet> andConditionSets) {
      conjunctions.addAll(
          andConditionSets.stream()
              .map(AndConditionSet::getConditions)
              .collect(Collectors.toSet()));
    }
  }

  static Selection addConjunctionsTo(Selection selection, Where where) {

    if (where.condition != null) {
      assert where.conjunctions.isEmpty() && where.disjunctions.isEmpty();
      selection.withConjunctions(ImmutableSet.of(Conjunction.of(where.condition)));
    } else if (where.conjunctions.isEmpty()) {
      selection.withConjunctions(
          Sets.cartesianProduct(new ArrayList<>(where.disjunctions)).stream()
              .filter(conditions -> conditions.size() > 0)
              .map(Conjunction::of)
              .collect(Collectors.toSet()));
    } else {
      selection.withConjunctions(
          where.conjunctions.stream()
              .filter(conditions -> conditions.size() > 0)
              .map(Conjunction::of)
              .collect(Collectors.toSet()));
    }

    return selection;
  }
}
