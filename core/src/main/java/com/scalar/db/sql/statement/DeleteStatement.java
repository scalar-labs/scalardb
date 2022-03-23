package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Predicate;

public class DeleteStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Predicate> predicates;

  public DeleteStatement(
      String namespaceName, String tableName, ImmutableList<Predicate> predicates) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.predicates = predicates;
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
