package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.Condition;
import com.scalar.db.sql.statement.UpdateStatement;

public final class UpdateBuilder {
  private UpdateBuilder() {}

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
      return new Where(
          namespaceName,
          tableName,
          assignments,
          whereConditionsBuilder,
          ImmutableList.builder(),
          false);
    }
  }

  public static class Where extends IfCondition {
    private Where(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      super(
          namespaceName,
          tableName,
          assignments,
          whereConditionsBuilder,
          ifConditionsBuilder,
          ifExists);
    }

    public Where and(Condition condition) {
      whereConditionsBuilder.add(condition);
      return this;
    }
  }

  public static class IfCondition extends End {
    private IfCondition(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      super(
          namespaceName,
          tableName,
          assignments,
          whereConditionsBuilder,
          ifConditionsBuilder,
          ifExists);
    }

    public If if_(Condition condition) {
      ifConditionsBuilder.add(condition);
      return new If(
          namespaceName,
          tableName,
          assignments,
          whereConditionsBuilder,
          ifConditionsBuilder,
          false);
    }

    public End ifExists() {
      ifExists = true;
      return this;
    }
  }

  public static class If extends End {
    private If(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      super(
          namespaceName,
          tableName,
          assignments,
          whereConditionsBuilder,
          ifConditionsBuilder,
          ifExists);
    }

    public If and(Condition condition) {
      whereConditionsBuilder.add(condition);
      return this;
    }
  }

  public static class End {
    protected final String namespaceName;
    protected final String tableName;
    protected final ImmutableList<Assignment> assignments;
    protected final ImmutableList.Builder<Condition> whereConditionsBuilder;
    protected final ImmutableList.Builder<Condition> ifConditionsBuilder;
    protected boolean ifExists;

    public End(
        String namespaceName,
        String tableName,
        ImmutableList<Assignment> assignments,
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.assignments = assignments;
      this.whereConditionsBuilder = whereConditionsBuilder;
      this.ifConditionsBuilder = ifConditionsBuilder;
      this.ifExists = ifExists;
    }

    public UpdateStatement build() {
      return new UpdateStatement(
          namespaceName,
          tableName,
          assignments,
          whereConditionsBuilder.build(),
          ifConditionsBuilder.build(),
          ifExists);
    }
  }
}
