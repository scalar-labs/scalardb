package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropNamespaceStatement;

public class DropNamespaceStatementBuilder {

  private DropNamespaceStatementBuilder() {}

  public static class Start extends Cascade {
    Start(String namespaceName) {
      super(namespaceName, false);
    }

    public Cascade ifExists() {
      return new Cascade(namespaceName, true);
    }
  }

  public static class Cascade extends Buildable {
    private Cascade(String namespaceName, boolean ifExists) {
      super(namespaceName, ifExists, false);
    }

    public Buildable cascade() {
      return new Buildable(namespaceName, ifExists, true);
    }
  }

  public static class Buildable {
    protected final String namespaceName;
    protected final boolean ifExists;
    private final boolean cascade;

    private Buildable(String namespaceName, boolean ifExists, boolean cascade) {
      this.namespaceName = namespaceName;
      this.ifExists = ifExists;
      this.cascade = cascade;
    }

    public DropNamespaceStatement build() {
      return DropNamespaceStatement.of(namespaceName, ifExists, cascade);
    }
  }
}
