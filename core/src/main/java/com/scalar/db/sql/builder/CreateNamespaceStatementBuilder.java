package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import java.util.Map;

public class CreateNamespaceStatementBuilder {

  private CreateNamespaceStatementBuilder() {}

  public static class Start extends Buildable {
    Start(String namespaceName) {
      super(namespaceName, false);
    }

    public Buildable ifNotExists() {
      return new Buildable(namespaceName, true);
    }

    public Buildable ifNotExists(boolean ifNotExists) {
      return new Buildable(namespaceName, ifNotExists);
    }
  }

  public static class Buildable {
    protected final String namespaceName;
    private final boolean ifNotExists;
    private ImmutableMap.Builder<String, String> optionsBuilder;

    private Buildable(String namespaceName, boolean ifNotExists) {
      this.namespaceName = namespaceName;
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

    public CreateNamespaceStatement build() {
      return CreateNamespaceStatement.of(
          namespaceName,
          ifNotExists,
          optionsBuilder == null ? ImmutableMap.of() : optionsBuilder.build());
    }
  }
}
