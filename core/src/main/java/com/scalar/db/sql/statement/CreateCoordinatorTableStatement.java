package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableMap;

public class CreateCoordinatorTableStatement implements DdlStatement {

  public final boolean ifNotExists;
  public final ImmutableMap<String, String> options;

  public CreateCoordinatorTableStatement(
      boolean ifNotExists, ImmutableMap<String, String> options) {
    this.ifNotExists = ifNotExists;
    this.options = options;
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
