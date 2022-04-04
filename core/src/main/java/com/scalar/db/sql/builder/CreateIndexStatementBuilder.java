package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.statement.CreateIndexStatement;
import java.util.Map;

public class CreateIndexStatementBuilder {

  private CreateIndexStatementBuilder() {}

  public static class Start extends OnTable {
    Start() {
      super(false);
    }

    public OnTable ifNotExists() {
      return new OnTable(true);
    }

    public OnTable ifNotExists(boolean ifNotExists) {
      return new OnTable(ifNotExists);
    }
  }

  public static class OnTable {
    private final boolean ifNotExists;

    private OnTable(boolean ifNotExists) {
      this.ifNotExists = ifNotExists;
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

    public Buildable column(String columnName) {
      return new Buildable(namespaceName, tableName, columnName, ifNotExists);
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;
    private final String columnName;
    private final boolean ifNotExists;
    private final ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();

    private Buildable(
        String namespaceName, String tableName, String columnName, boolean ifNotExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.ifNotExists = ifNotExists;
    }

    public Buildable withOption(String name, String value) {
      optionsBuilder.put(name, value);
      return this;
    }

    public Buildable withOptions(Map<String, String> options) {
      optionsBuilder.putAll(options);
      return this;
    }

    public CreateIndexStatement build() {
      return CreateIndexStatement.of(
          namespaceName, tableName, columnName, ifNotExists, optionsBuilder.build());
    }
  }
}
