package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Condition;

public class DeleteStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Condition> whereConditions;

  public DeleteStatement(
      String namespaceName, String tableName, ImmutableList<Condition> whereConditions) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.whereConditions = whereConditions;
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
