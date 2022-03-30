package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.TruncateCoordinatorTableStatement;

public class TruncateCoordinatorTableStatementBuilder {

  private TruncateCoordinatorTableStatementBuilder() {}

  public static class Start extends Buildable {
    Start() {}
  }

  public static class Buildable {
    private Buildable() {}

    public TruncateCoordinatorTableStatement build() {
      return TruncateCoordinatorTableStatement.of();
    }
  }
}
