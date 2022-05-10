package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropCoordinatorTablesStatement;

public class DropCoordinatorTablesStatementBuilder {

  private DropCoordinatorTablesStatementBuilder() {}

  public static class Start extends Buildable {
    Start() {
      super(false);
    }

    public Buildable ifExist() {
      return new Buildable(true);
    }

    public Buildable ifExist(boolean ifExist) {
      return new Buildable(ifExist);
    }
  }

  public static class Buildable {
    private final boolean ifExist;

    private Buildable(boolean ifExist) {
      this.ifExist = ifExist;
    }

    public DropCoordinatorTablesStatement build() {
      return DropCoordinatorTablesStatement.of(ifExist);
    }
  }
}
