package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateNamespaceStatement;

public class CreateNamespaceBuilder {

  private final String namespaceName;
  private boolean ifNotExists;
  private final ImmutableMap.Builder<String, String> optionsBuilder;

  CreateNamespaceBuilder(String namespaceName) {
    this.namespaceName = namespaceName;
    optionsBuilder = ImmutableMap.builder();
  }

  public CreateNamespaceBuilder ifNotExists() {
    ifNotExists = true;
    return this;
  }

  public CreateNamespaceBuilder withOption(String name, String value) {
    optionsBuilder.put(name, value);
    return this;
  }

  public CreateNamespaceStatement build() {
    return new CreateNamespaceStatement(namespaceName, ifNotExists, optionsBuilder.build());
  }
}
