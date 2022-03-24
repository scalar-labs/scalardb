package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Predicate;
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

    public OngoingWhere where(Predicate predicate) {
      ImmutableList.Builder<Predicate> predicateBuilder = ImmutableList.builder();
      predicateBuilder.add(predicate);
      return new OngoingWhere(namespaceName, tableName, predicateBuilder);
    }

    public Buildable where(List<Predicate> predicates) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.addAll(predicates);
      return new Buildable(namespaceName, tableName, predicatesBuilder);
    }
  }

  public static class OngoingWhere extends Buildable {
    private OngoingWhere(
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      super(namespaceName, tableName, predicatesBuilder);
    }

    public OngoingWhere and(Predicate predicate) {
      predicatesBuilder.add(predicate);
      return this;
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;
    protected final ImmutableList.Builder<Predicate> predicatesBuilder;

    private Buildable(
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.predicatesBuilder = predicatesBuilder;
    }

    public DeleteStatement build() {
      return DeleteStatement.of(namespaceName, tableName, predicatesBuilder.build());
    }
  }
}
