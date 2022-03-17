package com.scalar.db.sql.statement;

public class DropTableStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final boolean ifExists;

  public DropTableStatement(String namespaceName, String tableName, boolean ifExists) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
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
