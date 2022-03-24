package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.TruncateTableStatement;

public class TruncateTableStatementBuilder {

  private TruncateTableStatementBuilder() {}

  public static class Start extends Buildable {
    Start(String namespaceName, String tableName) {
      super(namespaceName, tableName);
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;

    private Buildable(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public TruncateTableStatement build() {
      return new TruncateTableStatement(namespaceName, tableName);
    }
  }
}
