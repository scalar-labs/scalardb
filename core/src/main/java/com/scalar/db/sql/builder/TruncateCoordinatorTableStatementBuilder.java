package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.TruncateCoordinatorTableStatement;

public class TruncateCoordinatorTableStatementBuilder {

  TruncateCoordinatorTableStatementBuilder() {}

  public TruncateCoordinatorTableStatement build() {
    return new TruncateCoordinatorTableStatement();
  }
}
