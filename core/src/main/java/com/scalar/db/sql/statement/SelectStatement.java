package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Ordering;
import com.scalar.db.sql.Predicate;

public class SelectStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<String> projectedColumnNames;
  public final ImmutableList<Predicate> predicates;
  public final ImmutableList<Ordering> orderings;
  public final int limit;

  public SelectStatement(
      String namespaceName,
      String tableName,
      ImmutableList<String> projectedColumnNames,
      ImmutableList<Predicate> predicates,
      ImmutableList<Ordering> orderings,
      int limit) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.projectedColumnNames = projectedColumnNames;
    this.predicates = predicates;
    this.orderings = orderings;
    this.limit = limit;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DmlStatementVisitor visitor) {
    visitor.visit(this);
  }
}
