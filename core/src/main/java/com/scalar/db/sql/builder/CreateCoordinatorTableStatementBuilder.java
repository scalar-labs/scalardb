package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateCoordinatorTableStatement;

public class CreateCoordinatorTableStatementBuilder {

  private boolean ifNotExists;
  private final ImmutableMap.Builder<String, String> optionsBuilder;

  CreateCoordinatorTableStatementBuilder() {
    optionsBuilder = ImmutableMap.builder();
  }

  public CreateCoordinatorTableStatementBuilder ifNotExists() {
    ifNotExists = true;
    return this;
  }

  public CreateCoordinatorTableStatementBuilder withOption(String name, String value) {
    optionsBuilder.put(name, value);
    return this;
  }

  public CreateCoordinatorTableStatement build() {
    return new CreateCoordinatorTableStatement(ifNotExists, optionsBuilder.build());
  }
}
