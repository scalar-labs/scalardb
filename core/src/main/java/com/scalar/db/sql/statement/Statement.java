package com.scalar.db.sql.statement;

public interface Statement {
  <R, C> R accept(StatementVisitor<R, C> visitor, C context);
}
