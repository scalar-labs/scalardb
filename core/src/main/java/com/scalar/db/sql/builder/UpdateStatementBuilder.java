package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.Condition;
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

    public Where where(Condition condition) {
      ImmutableList.Builder<Condition> whereConditionsBuilder = ImmutableList.builder();
      whereConditionsBuilder.add(condition);
      return new Where(namespaceName, tableName, assignments, whereConditionsBuilder);
    }

    public End where(List<Condition> conditions) {
      ImmutableList.Builder<Condition> whereConditionsBuilder = ImmutableList.builder();
      whereConditionsBuilder.addAll(conditions);
      return new End(namespaceName, tableName, assignments, whereConditionsBuilder);
    }
  }

  public static class Where extends End {
    private Where(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Condition> whereConditionsBuilder) {
      super(namespaceName, tableName, assignments, whereConditionsBuilder);
    }

    public Where and(Condition condition) {
      whereConditionsBuilder.add(condition);
      return this;
    }
  }

  public static class End {
    protected final String namespaceName;
    protected final String tableName;
    protected final ImmutableList<Assignment> assignments;
    protected final ImmutableList.Builder<Condition> whereConditionsBuilder;

    public End(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Condition> whereConditionsBuilder) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.assignments = assignments;
      this.whereConditionsBuilder = whereConditionsBuilder;
    }

    public UpdateStatement build() {
      return new UpdateStatement(
          namespaceName, tableName, assignments, whereConditionsBuilder.build());
    }
  }
}
