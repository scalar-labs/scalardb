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

    public Where where(Predicate predicate) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.add(predicate);
      return new Where(namespaceName, tableName, assignments, predicatesBuilder);
    }

    public End where(List<Predicate> predicates) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.addAll(predicates);
      return new End(namespaceName, tableName, assignments, predicatesBuilder);
    }
  }

  public static class Where extends End {
    private Where(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      super(namespaceName, tableName, assignments, predicatesBuilder);
    }

    public Where and(Predicate predicate) {
      predicatesBuilder.add(predicate);
      return this;
    }
  }

  public static class End {
    protected final String namespaceName;
    protected final String tableName;
    protected final ImmutableList<Assignment> assignments;
    protected final ImmutableList.Builder<Predicate> predicatesBuilder;

    public End(
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
      return new UpdateStatement(namespaceName, tableName, assignments, predicatesBuilder.build());
    }
  }
}
