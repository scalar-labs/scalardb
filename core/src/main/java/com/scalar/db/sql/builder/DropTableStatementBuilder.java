package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropTableStatement;

public class DropTableStatementBuilder {

  private final String namespaceName;
  private final String tableName;
  private boolean ifExists;

  DropTableStatementBuilder(String namespaceName, String tableName) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
  }

  public DropTableStatementBuilder ifExists() {
    ifExists = true;
    return this;
  }

  public DropTableStatement build() {
    return new DropTableStatement(namespaceName, tableName, ifExists);
  }
}
