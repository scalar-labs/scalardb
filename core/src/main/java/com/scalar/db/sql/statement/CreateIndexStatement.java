package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableMap;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CreateIndexStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final String columnName;
  public final boolean ifNotExists;
  public final ImmutableMap<String, String> options;

  public CreateIndexStatement(
      String namespaceName,
      String tableName,
      String columnName,
      boolean ifNotExists,
      ImmutableMap<String, String> options) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.columnName = columnName;
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
