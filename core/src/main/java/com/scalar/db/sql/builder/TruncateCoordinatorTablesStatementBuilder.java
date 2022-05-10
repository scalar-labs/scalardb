package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.TruncateCoordinatorTablesStatement;

public class TruncateCoordinatorTablesStatementBuilder {

  private TruncateCoordinatorTablesStatementBuilder() {}

  public static class Start extends Buildable {
    Start() {}
  }

  public static class Buildable {
    private Buildable() {}

    public TruncateCoordinatorTablesStatement build() {
      return TruncateCoordinatorTablesStatement.of();
    }
  }
}
