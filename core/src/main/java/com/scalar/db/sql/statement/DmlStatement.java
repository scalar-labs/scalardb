package com.scalar.db.sql.statement;

public interface DmlStatement extends Statement {
  void accept(DmlStatementVisitor visitor);
}
