package com.scalar.db.sql.statement;

public class TruncateCoordinatorTableStatement implements DdlStatement {

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DdlStatementVisitor visitor) {
    visitor.visit(this);
  }
}
