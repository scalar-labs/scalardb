package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Condition;
import com.scalar.db.sql.statement.DeleteStatement;

public final class DeleteBuilder {

  private DeleteBuilder() {}

  public static class Start extends WhereStart {
    Start(String namespaceName, String tableName) {
      super(namespaceName, tableName);
    }
  }

  public static class WhereStart {
    private final String namespaceName;
    private final String tableName;

    private WhereStart(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public Where where(Condition condition) {
      ImmutableList.Builder<Condition> whereConditionsBuilder = ImmutableList.builder();
      whereConditionsBuilder.add(condition);
      return new Where(
          namespaceName, tableName, whereConditionsBuilder, ImmutableList.builder(), false);
    }
  }

  public static class Where extends IfCondition {
    private Where(
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      super(namespaceName, tableName, whereConditionsBuilder, ifConditionsBuilder, ifExists);
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
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      super(namespaceName, tableName, whereConditionsBuilder, ifConditionsBuilder, ifExists);
    }

    public If if_(Condition condition) {
      ifConditionsBuilder.add(condition);
      return new If(namespaceName, tableName, whereConditionsBuilder, ifConditionsBuilder, false);
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
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      super(namespaceName, tableName, whereConditionsBuilder, ifConditionsBuilder, ifExists);
    }

    public If and(Condition condition) {
      ifConditionsBuilder.add(condition);
      return this;
    }
  }

  public static class End {
    protected final String namespaceName;
    protected final String tableName;
    protected final ImmutableList.Builder<Condition> whereConditionsBuilder;
    protected final ImmutableList.Builder<Condition> ifConditionsBuilder;
    protected boolean ifExists;

    public End(
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Condition> whereConditionsBuilder,
        ImmutableList.Builder<Condition> ifConditionsBuilder,
        boolean ifExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.whereConditionsBuilder = whereConditionsBuilder;
      this.ifConditionsBuilder = ifConditionsBuilder;
      this.ifExists = ifExists;
    }

    public DeleteStatement build() {
      return new DeleteStatement(
          namespaceName,
          tableName,
          whereConditionsBuilder.build(),
          ifConditionsBuilder.build(),
          ifExists);
    }
  }
}
