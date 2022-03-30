package com.scalar.db.sql.statement;

public interface DmlStatement extends Statement {
  <R, C> R accept(DmlStatementVisitor<R, C> visitor, C context);
}
