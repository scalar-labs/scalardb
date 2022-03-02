package com.scalar.db.sql.statement;

public interface Statement {
  void accept(StatementVisitor visitor);
}
