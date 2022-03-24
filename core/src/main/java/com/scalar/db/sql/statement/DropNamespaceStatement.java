package com.scalar.db.sql.statement;

import javax.annotation.concurrent.Immutable;

@Immutable
public class DropNamespaceStatement implements DdlStatement {

  public final String namespaceName;
  public final boolean ifExists;
  public final boolean cascade;

  public DropNamespaceStatement(String namespaceName, boolean ifExists, boolean cascade) {
    this.namespaceName = namespaceName;
    this.ifExists = ifExists;
    this.cascade = cascade;
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
