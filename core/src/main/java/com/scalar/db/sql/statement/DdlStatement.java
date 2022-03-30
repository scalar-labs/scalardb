package com.scalar.db.sql.statement;

public interface DdlStatement extends Statement {
  <R, C> R accept(DdlStatementVisitor<R, C> visitor, C context);
}
