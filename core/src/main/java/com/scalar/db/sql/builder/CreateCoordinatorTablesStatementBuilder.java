package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateCoordinatorTablesStatement;
import java.util.Map;

public class CreateCoordinatorTablesStatementBuilder {

  private CreateCoordinatorTablesStatementBuilder() {}

  public static class Start extends Buildable {
    Start() {
      super(false);
    }

    public Buildable ifNotExist() {
      return new Buildable(true);
    }

    public Buildable ifNotExist(boolean ifNotExist) {
      return new Buildable(ifNotExist);
    }
  }

  public static class Buildable {
    private final boolean ifNotExist;
    private ImmutableMap.Builder<String, String> optionsBuilder;

    private Buildable(boolean ifNotExist) {
      this.ifNotExist = ifNotExist;
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

    public CreateCoordinatorTablesStatement build() {
      return CreateCoordinatorTablesStatement.of(
          ifNotExist, optionsBuilder == null ? ImmutableMap.of() : optionsBuilder.build());
    }
  }
}
