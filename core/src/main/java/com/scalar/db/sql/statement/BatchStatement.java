package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;

public class BatchStatement implements Statement {

  public final ImmutableList<BatchableStatement> statements;

  public BatchStatement(ImmutableList<BatchableStatement> statements) {
    this.statements = statements;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }
}
