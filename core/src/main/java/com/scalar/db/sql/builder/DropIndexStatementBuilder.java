package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropIndexStatement;

public class DropIndexStatementBuilder {

  private DropIndexStatementBuilder() {}

  public static class Start extends OnTable {
    Start() {
      super(false);
    }

    public OnTable ifExists() {
      return new OnTable(true);
    }
  }

  public static class OnTable {
    private final boolean ifExists;

    private OnTable(boolean ifExists) {
      this.ifExists = ifExists;
    }

    public Column onTable(String namespaceName, String tableName) {
      return new Column(namespaceName, tableName, ifExists);
    }
  }

  public static class Column {
    private final String namespaceName;
    private final String tableName;
    private final boolean ifExists;

    private Column(String namespaceName, String tableName, boolean ifExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.ifExists = ifExists;
    }

    public Buildable column(String columnName) {
      return new Buildable(namespaceName, tableName, columnName, ifExists);
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;
    private final String columnName;
    private final boolean ifExists;

    private Buildable(String namespaceName, String tableName, String columnName, boolean ifExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.ifExists = ifExists;
    }

    public DropIndexStatement build() {
      return DropIndexStatement.of(namespaceName, tableName, columnName, ifExists);
    }
  }
}
