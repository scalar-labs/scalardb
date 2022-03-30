package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropTableStatement;

public class DropTableStatementBuilder {

  private DropTableStatementBuilder() {}

  public static class Start extends Buildable {
    Start(String namespaceName, String tableName) {
      super(namespaceName, tableName, false);
    }

    public Buildable ifExists() {
      return new Buildable(namespaceName, tableName, true);
    }
  }

  public static class Buildable {
    protected final String namespaceName;
    protected final String tableName;
    private final boolean ifExists;

    private Buildable(String namespaceName, String tableName, boolean ifExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.ifExists = ifExists;
    }

    public DropTableStatement build() {
      return DropTableStatement.of(namespaceName, tableName, ifExists);
    }
  }
}
