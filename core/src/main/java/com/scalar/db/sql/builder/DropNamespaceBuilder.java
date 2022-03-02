package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropNamespaceStatement;

public class DropNamespaceBuilder {

  private final String namespaceName;
  private boolean ifExists;
  private boolean cascade;

  DropNamespaceBuilder(String namespaceName) {
    this.namespaceName = namespaceName;
  }

  public DropNamespaceBuilder ifExists() {
    ifExists = true;
    return this;
  }

  public DropNamespaceBuilder cascade() {
    cascade = true;
    return this;
  }

  public DropNamespaceStatement build() {
    return new DropNamespaceStatement(namespaceName, ifExists, cascade);
  }
}
