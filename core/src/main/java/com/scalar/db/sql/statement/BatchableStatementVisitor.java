package com.scalar.db.sql.statement;

public interface BatchableStatementVisitor {
  void visit(InsertStatement statement);

  void visit(UpdateStatement statement);

  void visit(DeleteStatement statement);
}
