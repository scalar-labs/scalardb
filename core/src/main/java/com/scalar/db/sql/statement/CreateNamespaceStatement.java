package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableMap;

public class CreateNamespaceStatement implements Statement {

  public final String namespaceName;
  public final boolean ifNotExists;
  public final ImmutableMap<String, String> options;

  public CreateNamespaceStatement(
      String namespaceName, boolean ifNotExists, ImmutableMap<String, String> options) {
    this.namespaceName = namespaceName;
    this.ifNotExists = ifNotExists;
    this.options = options;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }
}
