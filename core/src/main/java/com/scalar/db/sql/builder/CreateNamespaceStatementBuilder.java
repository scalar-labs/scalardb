package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateNamespaceStatement;

public class CreateNamespaceStatementBuilder {

  private final String namespaceName;
  private boolean ifNotExists;
  private final ImmutableMap.Builder<String, String> optionsBuilder;

  CreateNamespaceStatementBuilder(String namespaceName) {
    this.namespaceName = namespaceName;
    optionsBuilder = ImmutableMap.builder();
  }

  public CreateNamespaceStatementBuilder ifNotExists() {
    ifNotExists = true;
    return this;
  }

  public CreateNamespaceStatementBuilder withOption(String name, String value) {
    optionsBuilder.put(name, value);
    return this;
  }

  public CreateNamespaceStatement build() {
    return new CreateNamespaceStatement(namespaceName, ifNotExists, optionsBuilder.build());
  }
}
