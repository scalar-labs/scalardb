package com.scalar.db.sql.statement;

public interface BatchableStatement {
  void accept(BatchableStatementVisitor visitor);
}
