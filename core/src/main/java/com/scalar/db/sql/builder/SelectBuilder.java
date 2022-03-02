package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Condition;
import com.scalar.db.sql.Ordering;
import com.scalar.db.sql.statement.SelectStatement;
import java.util.List;

public final class SelectBuilder {

  private SelectBuilder() {}

  public static class Start {
    private final ImmutableList<String> projectedColumnNames;

    Start(List<String> projectedColumnNames) {
      this.projectedColumnNames = ImmutableList.copyOf(projectedColumnNames);
    }

    public WhereStart from(String namespaceName, String tableName) {
      return new WhereStart(projectedColumnNames, namespaceName, tableName);
    }
  }

  public static class WhereStart {
    private final ImmutableList<String> projectedColumnNames;
    private final String namespaceName;
    private final String tableName;

    private WhereStart(
        ImmutableList<String> projectedColumnNames, String namespaceName, String tableName) {
      this.projectedColumnNames = projectedColumnNames;
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public Where where(Condition condition) {
      ImmutableList.Builder<Condition> whereConditionsBuilder = ImmutableList.builder();
      whereConditionsBuilder.add(condition);
      return new Where(projectedColumnNames, namespaceName, tableName, whereConditionsBuilder);
    }
  }

  public static class Where extends End {
    public Where(
        ImmutableList<String> projectedColumnNames,
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Condition> whereConditionsBuilder) {
      super(projectedColumnNames, namespaceName, tableName, whereConditionsBuilder);
    }

    public Where and(Condition condition) {
      whereConditionsBuilder.add(condition);
      return this;
    }
  }

  public static class End {
    protected final ImmutableList<String> projectedColumnNames;
    protected final String namespaceName;
    protected final String tableName;
    protected final ImmutableList.Builder<Condition> whereConditionsBuilder;
    private ImmutableList<Ordering> orderings = ImmutableList.of();
    private int limit;

    public End(
        ImmutableList<String> projectedColumnNames,
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Condition> whereConditionsBuilder) {
      this.projectedColumnNames = projectedColumnNames;
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.whereConditionsBuilder = whereConditionsBuilder;
    }

    public End orderBy(Ordering... orderings) {
      this.orderings = ImmutableList.copyOf(orderings);
      return this;
    }

    public End limit(int limit) {
      this.limit = limit;
      return this;
    }

    public SelectStatement build() {
      return new SelectStatement(
          namespaceName,
          tableName,
          projectedColumnNames,
          whereConditionsBuilder.build(),
          orderings,
          limit);
    }
  }
}
