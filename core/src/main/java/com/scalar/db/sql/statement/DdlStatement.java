package com.scalar.db.sql.statement;

public interface DdlStatement extends Statement {
  void accept(DdlStatementVisitor visitor);
}
