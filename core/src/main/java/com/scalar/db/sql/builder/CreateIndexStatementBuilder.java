package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateIndexStatement;

public class CreateIndexStatementBuilder {

  private CreateIndexStatementBuilder() {}

  public static class Start {
    private boolean ifNotExists;

    Start() {}

    public Start ifNotExists() {
      ifNotExists = true;
      return this;
    }

    public Column onTable(String namespaceName, String tableName) {
      return new Column(namespaceName, tableName, ifNotExists);
    }
  }

  public static class Column {
    private final String namespaceName;
    private final String tableName;
    private final boolean ifNotExists;

    private Column(String namespaceName, String tableName, boolean ifNotExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.ifNotExists = ifNotExists;
    }

    public End column(String columnName) {
      return new End(namespaceName, tableName, columnName, ifNotExists);
    }
  }

  public static class End {
    private final String namespaceName;
    private final String tableName;
    private final String columnName;
    private final boolean ifNotExists;
    private final ImmutableMap.Builder<String, String> optionsBuilder;

    private End(String namespaceName, String tableName, String columnName, boolean ifNotExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.ifNotExists = ifNotExists;

      optionsBuilder = ImmutableMap.builder();
    }

    public End withOption(String name, String value) {
      optionsBuilder.put(name, value);
      return this;
    }

    public CreateIndexStatement build() {
      return new CreateIndexStatement(
          namespaceName, tableName, columnName, ifNotExists, optionsBuilder.build());
    }
  }
}
