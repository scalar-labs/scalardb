package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.statement.UpdateStatement;
import java.util.List;

public final class UpdateStatementBuilder {

  private UpdateStatementBuilder() {}

  public static class Start {
    private final String namespaceName;
    private final String tableName;

    Start(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public WhereStart set(Assignment... assignments) {
      return new WhereStart(namespaceName, tableName, ImmutableList.copyOf(assignments));
    }

    public WhereStart set(List<Assignment> assignments) {
      return new WhereStart(namespaceName, tableName, ImmutableList.copyOf(assignments));
    }
  }

  public static class WhereStart {
    private final String namespaceName;
    private final String tableName;
    private final ImmutableList<Assignment> assignments;

    private WhereStart(
        String namespaceName, String tableName, ImmutableList<Assignment> assignments) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.assignments = assignments;
    }

    public OngoingWhere where(Predicate predicate) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.add(predicate);
      return new OngoingWhere(namespaceName, tableName, assignments, predicatesBuilder);
    }

    public Buildable where(List<Predicate> predicates) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.addAll(predicates);
      return new Buildable(namespaceName, tableName, assignments, predicatesBuilder);
    }
  }

  public static class OngoingWhere extends Buildable {
    private OngoingWhere(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      super(namespaceName, tableName, assignments, predicatesBuilder);
    }

    public OngoingWhere and(Predicate predicate) {
      predicatesBuilder.add(predicate);
      return this;
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;
    private final ImmutableList<Assignment> assignments;
    protected final ImmutableList.Builder<Predicate> predicatesBuilder;

    private Buildable(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.assignments = assignments;
      this.predicatesBuilder = predicatesBuilder;
    }

    public UpdateStatement build() {
      return UpdateStatement.of(namespaceName, tableName, assignments, predicatesBuilder.build());
    }
  }
}
