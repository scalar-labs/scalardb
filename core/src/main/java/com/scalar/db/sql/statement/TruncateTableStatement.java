package com.scalar.db.sql.statement;

import javax.annotation.concurrent.Immutable;

@Immutable
public class TruncateTableStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;

  public TruncateTableStatement(String namespaceName, String tableName) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
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
