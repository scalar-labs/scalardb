package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropTableStatement;

public class DropTableBuilder {

  private final String namespaceName;
  private final String tableName;
  private boolean ifExists;

  DropTableBuilder(String namespaceName, String tableName) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
  }

  public DropTableBuilder ifExists() {
    ifExists = true;
    return this;
  }

  public DropTableStatement build() {
    return new DropTableStatement(namespaceName, tableName, ifExists);
  }
}
