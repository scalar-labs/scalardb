package com.scalar.db.sql.statement;

import javax.annotation.concurrent.Immutable;

@Immutable
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
