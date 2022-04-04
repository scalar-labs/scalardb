package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropCoordinatorTableStatement;

public class DropCoordinatorTableStatementBuilder {

  private DropCoordinatorTableStatementBuilder() {}

  public static class Start extends Buildable {
    Start() {
      super(false);
    }

    public Buildable ifExists() {
      return new Buildable(true);
    }

    public Buildable ifExists(boolean ifExists) {
      return new Buildable(ifExists);
    }
  }

  public static class Buildable {
    private final boolean ifExists;

    private Buildable(boolean ifExists) {
      this.ifExists = ifExists;
    }

    public DropCoordinatorTableStatement build() {
      return DropCoordinatorTableStatement.of(ifExists);
    }
  }
}
