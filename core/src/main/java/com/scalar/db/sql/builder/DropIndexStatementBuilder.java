package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropIndexStatement;

public class DropIndexStatementBuilder {

  private DropIndexStatementBuilder() {}

  public static class Start {
    private boolean ifExists;

    Start() {}

    public Start ifExists() {
      ifExists = true;
      return this;
    }

    public Column onTable(String namespaceName, String tableName) {
      return new Column(namespaceName, tableName, ifExists);
    }
  }

  public static class Column {
    private final String namespaceName;
    private final String tableName;
    private final boolean ifExists;

    private Column(String namespaceName, String tableName, boolean ifNotExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.ifExists = ifNotExists;
    }

    public End column(String columnName) {
      return new End(namespaceName, tableName, columnName, ifExists);
    }
  }

  public static class End {
    private final String namespaceName;
    private final String tableName;
    private final String columnName;
    private final boolean ifExists;

    private End(String namespaceName, String tableName, String columnName, boolean ifExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.ifExists = ifExists;
    }

    public DropIndexStatement build() {
      return new DropIndexStatement(namespaceName, tableName, columnName, ifExists);
    }
  }
}
