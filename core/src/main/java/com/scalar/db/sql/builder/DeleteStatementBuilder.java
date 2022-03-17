package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Condition;
import com.scalar.db.sql.statement.DeleteStatement;
import java.util.List;

public final class DeleteStatementBuilder {

  private DeleteStatementBuilder() {}

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
      return new Where(namespaceName, tableName, whereConditionsBuilder);
    }

    public End where(List<Condition> conditions) {
      ImmutableList.Builder<Condition> whereConditionsBuilder = ImmutableList.builder();
      whereConditionsBuilder.addAll(conditions);
      return new End(namespaceName, tableName, whereConditionsBuilder);
    }
  }

  public static class Where extends End {
    private Where(
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Condition> whereConditionsBuilder) {
      super(namespaceName, tableName, whereConditionsBuilder);
    }

    public Where and(Condition condition) {
      whereConditionsBuilder.add(condition);
      return this;
    }
  }

  public static class End {
    protected final String namespaceName;
    protected final String tableName;
    protected final ImmutableList.Builder<Condition> whereConditionsBuilder;

    public End(
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Condition> whereConditionsBuilder) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.whereConditionsBuilder = whereConditionsBuilder;
    }

    public DeleteStatement build() {
      return new DeleteStatement(namespaceName, tableName, whereConditionsBuilder.build());
    }
  }
}
