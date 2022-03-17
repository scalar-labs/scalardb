package com.scalar.db.sql.statement;

public class DropCoordinatorTableStatement implements DdlStatement {

  public final boolean ifExists;

  public DropCoordinatorTableStatement(boolean ifExists) {
    this.ifExists = ifExists;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DdlStatementVisitor visitor) {
    visitor.visit(this);
  }
}
