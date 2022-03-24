package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.ClusteringOrdering;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.statement.SelectStatement;
import java.util.List;

public final class SelectStatementBuilder {

  private SelectStatementBuilder() {}

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

    public OngoingWhere where(Predicate predicate) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.add(predicate);
      return new OngoingWhere(projectedColumnNames, namespaceName, tableName, predicatesBuilder);
    }

    public Buildable where(List<Predicate> predicates) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.addAll(predicates);
      return new Buildable(projectedColumnNames, namespaceName, tableName, predicatesBuilder);
    }
  }

  public static class OngoingWhere extends Buildable {
    private OngoingWhere(
        ImmutableList<String> projectedColumnNames,
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      super(projectedColumnNames, namespaceName, tableName, predicatesBuilder);
    }

    public OngoingWhere and(Predicate predicate) {
      predicatesBuilder.add(predicate);
      return this;
    }
  }

  public static class Buildable {
    private final ImmutableList<String> projectedColumnNames;
    private final String namespaceName;
    private final String tableName;
    protected final ImmutableList.Builder<Predicate> predicatesBuilder;
    private ImmutableList<ClusteringOrdering> clusteringOrderings = ImmutableList.of();
    private int limit;

    private Buildable(
        ImmutableList<String> projectedColumnNames,
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      this.projectedColumnNames = projectedColumnNames;
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.predicatesBuilder = predicatesBuilder;
    }

    public Buildable orderBy(ClusteringOrdering... clusteringOrderings) {
      this.clusteringOrderings = ImmutableList.copyOf(clusteringOrderings);
      return this;
    }

    public Buildable orderBy(List<ClusteringOrdering> clusteringOrderings) {
      this.clusteringOrderings = ImmutableList.copyOf(clusteringOrderings);
      return this;
    }

    public Buildable limit(int limit) {
      this.limit = limit;
      return this;
    }

    public SelectStatement build() {
      return new SelectStatement(
          namespaceName,
          tableName,
          projectedColumnNames,
          predicatesBuilder.build(),
          clusteringOrderings,
          limit);
    }
  }
}
