package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropNamespaceStatement;

public class DropNamespaceStatementBuilder {

  private final String namespaceName;
  private boolean ifExists;
  private boolean cascade;

  DropNamespaceStatementBuilder(String namespaceName) {
    this.namespaceName = namespaceName;
  }

  public DropNamespaceStatementBuilder ifExists() {
    ifExists = true;
    return this;
  }

  public DropNamespaceStatementBuilder cascade() {
    cascade = true;
    return this;
  }

  public DropNamespaceStatement build() {
    return new DropNamespaceStatement(namespaceName, ifExists, cascade);
  }
}
