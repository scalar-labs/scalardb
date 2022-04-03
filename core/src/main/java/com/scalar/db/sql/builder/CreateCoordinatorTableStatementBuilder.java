package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateCoordinatorTableStatement;
import java.util.Map;

public class CreateCoordinatorTableStatementBuilder {

  private CreateCoordinatorTableStatementBuilder() {}

  public static class Start extends Buildable {
    Start() {
      super(false);
    }

    public Buildable ifNotExists() {
      return new Buildable(true);
    }

    public Buildable ifNotExists(boolean ifNotExists) {
      return new Buildable(ifNotExists);
    }
  }

  public static class Buildable {
    private final boolean ifNotExists;
    private ImmutableMap.Builder<String, String> optionsBuilder;

    private Buildable(boolean ifNotExists) {
      this.ifNotExists = ifNotExists;
    }

    public Buildable withOption(String name, String value) {
      if (optionsBuilder == null) {
        optionsBuilder = ImmutableMap.builder();
      }
      optionsBuilder.put(name, value);
      return this;
    }

    public Buildable withOptions(Map<String, String> options) {
      if (optionsBuilder == null) {
        optionsBuilder = ImmutableMap.builder();
      }
      optionsBuilder.putAll(options);
      return this;
    }

    public CreateCoordinatorTableStatement build() {
      return CreateCoordinatorTableStatement.of(
          ifNotExists, optionsBuilder == null ? ImmutableMap.of() : optionsBuilder.build());
    }
  }
}
