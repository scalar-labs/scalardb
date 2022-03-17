package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.DropCoordinatorTableStatement;

public class DropCoordinatorTableStatementBuilder {

  private boolean ifExists;

  DropCoordinatorTableStatementBuilder() {}

  public DropCoordinatorTableStatementBuilder ifExists() {
    ifExists = true;
    return this;
  }

  public DropCoordinatorTableStatement build() {
    return new DropCoordinatorTableStatement(ifExists);
  }
}
