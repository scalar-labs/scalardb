package com.scalar.db.sql.statement;

public class DropNamespaceStatement implements Statement {

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
}
