package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.Predicate;

public class UpdateStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Assignment> assignments;
  public final ImmutableList<Predicate> wherePredicates;

  public UpdateStatement(
      String namespaceName,
      String tableName,
      ImmutableList<Assignment> assignments,
      ImmutableList<Predicate> wherePredicates) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.assignments = assignments;
    this.wherePredicates = wherePredicates;
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
