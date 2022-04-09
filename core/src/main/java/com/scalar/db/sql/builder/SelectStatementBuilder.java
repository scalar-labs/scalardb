package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.ClusteringOrdering;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.Projection;
import com.scalar.db.sql.Term;
import com.scalar.db.sql.Value;
import com.scalar.db.sql.statement.SelectStatement;
import java.util.List;

public final class SelectStatementBuilder {

  private SelectStatementBuilder() {}

  public static class Start {
    private final ImmutableList<Projection> projections;

    Start(List<Projection> projections) {
      this.projections = ImmutableList.copyOf(projections);
    }

    public WhereStart from(String namespaceName, String tableName) {
      return new WhereStart(projections, namespaceName, tableName);
    }
  }

  public static class WhereStart {
    private final ImmutableList<Projection> projections;
    private final String namespaceName;
    private final String tableName;

    private WhereStart(
        ImmutableList<Projection> projections, String namespaceName, String tableName) {
      this.projections = projections;
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public OngoingWhere where(Predicate predicate) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.add(predicate);
      return new OngoingWhere(projections, namespaceName, tableName, predicatesBuilder);
    }

    public Buildable where(List<Predicate> predicates) {
      ImmutableList.Builder<Predicate> predicatesBuilder = ImmutableList.builder();
      predicatesBuilder.addAll(predicates);
      return new Buildable(projections, namespaceName, tableName, predicatesBuilder);
    }
  }

  public static class OngoingWhere extends Buildable {
    private OngoingWhere(
        ImmutableList<Projection> projections,
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      super(projections, namespaceName, tableName, predicatesBuilder);
    }

    public OngoingWhere and(Predicate predicate) {
      predicatesBuilder.add(predicate);
      return this;
    }
  }

  public static class Buildable {
    private final ImmutableList<Projection> projections;
    private final String namespaceName;
    private final String tableName;
    protected final ImmutableList.Builder<Predicate> predicatesBuilder;
    private ImmutableList<ClusteringOrdering> clusteringOrderings = ImmutableList.of();
    private Term limit;

    private Buildable(
        ImmutableList<Projection> projections,
        String namespaceName,
        String tableName,
        ImmutableList.Builder<Predicate> predicatesBuilder) {
      this.projections = projections;
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
      this.limit = Value.ofInt(limit);
      return this;
    }

    public Buildable limit(Term limit) {
      this.limit = limit;
      return this;
    }

    public SelectStatement build() {
      return SelectStatement.of(
          namespaceName,
          tableName,
          projections,
          predicatesBuilder.build(),
          clusteringOrderings,
          limit);
    }
  }
}
