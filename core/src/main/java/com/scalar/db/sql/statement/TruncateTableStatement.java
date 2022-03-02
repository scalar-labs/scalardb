package com.scalar.db.sql.statement;

public class TruncateTableStatement implements Statement {

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
}
