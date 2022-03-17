package com.scalar.db.sql.builder;

import com.scalar.db.sql.statement.TruncateTableStatement;

public class TruncateTableStatementBuilder {

  private final String namespaceName;
  private final String tableName;

  TruncateTableStatementBuilder(String namespaceName, String tableName) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
  }

  public TruncateTableStatement build() {
    return new TruncateTableStatement(namespaceName, tableName);
  }
}
