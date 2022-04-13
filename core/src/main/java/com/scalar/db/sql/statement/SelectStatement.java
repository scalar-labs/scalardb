package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.BindMarker;
import com.scalar.db.sql.ClusteringOrdering;
import com.scalar.db.sql.NamedBindMarker;
import com.scalar.db.sql.PositionalBindMarker;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.Projection;
import com.scalar.db.sql.SqlUtils;
import com.scalar.db.sql.Term;
import com.scalar.db.sql.Value;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SelectStatement implements DmlStatement, BindableStatement<SelectStatement> {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Projection> projections;
  public final ImmutableList<Predicate> predicates;
  public final ImmutableList<ClusteringOrdering> clusteringOrderings;
  public final Term limit;

  private SelectStatement(
      String namespaceName,
      String tableName,
      ImmutableList<Projection> projections,
      ImmutableList<Predicate> predicates,
      ImmutableList<ClusteringOrdering> clusteringOrderings,
      Term limit) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.projections = Objects.requireNonNull(projections);
    this.predicates = Objects.requireNonNull(predicates);
    this.clusteringOrderings = Objects.requireNonNull(clusteringOrderings);
    this.limit = Objects.requireNonNull(limit);
  }

  @Override
  public SelectStatement bind(List<Value> positionalValues) {
    Iterator<Value> positionalValueIterator = positionalValues.iterator();
    return new SelectStatement(
        namespaceName,
        tableName,
        projections,
        SqlUtils.bindPredicates(predicates, positionalValueIterator),
        clusteringOrderings,
        bindLimit(limit, positionalValueIterator));
  }

  private Term bindLimit(Term limit, Iterator<Value> positionalValueIterator) {
    if (positionalValueIterator.hasNext() && limit instanceof BindMarker) {
      if (limit instanceof NamedBindMarker) {
        throw new IllegalArgumentException("A named bind marker is not allowed");
      }
      return positionalValueIterator.next();
    }
    return limit;
  }

  @Override
  public SelectStatement bind(Map<String, Value> namedValues) {
    return new SelectStatement(
        namespaceName,
        tableName,
        projections,
        SqlUtils.bindPredicates(predicates, namedValues),
        clusteringOrderings,
        bindLimit(limit, namedValues));
  }

  private Term bindLimit(Term limit, Map<String, Value> namedValues) {
    if (limit instanceof BindMarker) {
      if (limit instanceof PositionalBindMarker) {
        throw new IllegalArgumentException("A positional bind marker is not allowed");
      }
      String name = ((NamedBindMarker) limit).name;
      if (namedValues.containsKey(name)) {
        return namedValues.get(name);
      } else {
        return limit;
      }
    }
    return limit;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public <R, C> R accept(DmlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("projectedColumnNames", projections)
        .add("predicates", predicates)
        .add("clusteringOrderings", clusteringOrderings)
        .add("limit", limit)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SelectStatement)) {
      return false;
    }
    SelectStatement that = (SelectStatement) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(projections, that.projections)
        && Objects.equals(predicates, that.predicates)
        && Objects.equals(clusteringOrderings, that.clusteringOrderings)
        && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        namespaceName, tableName, projections, predicates, clusteringOrderings, limit);
  }

  public static SelectStatement of(
      String namespaceName,
      String tableName,
      ImmutableList<Projection> projections,
      ImmutableList<Predicate> predicates,
      ImmutableList<ClusteringOrdering> clusteringOrderings,
      Term limit) {
    return new SelectStatement(
        namespaceName, tableName, projections, predicates, clusteringOrderings, limit);
  }
}
