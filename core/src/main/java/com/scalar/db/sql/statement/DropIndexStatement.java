package com.scalar.db.sql.statement;

import javax.annotation.concurrent.Immutable;

@Immutable
public class DropIndexStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final String columnName;
  public final boolean ifExists;

  public DropIndexStatement(
      String namespaceName, String tableName, String columnName, boolean ifExists) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.columnName = columnName;
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
