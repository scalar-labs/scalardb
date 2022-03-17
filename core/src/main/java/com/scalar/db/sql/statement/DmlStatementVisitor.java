package com.scalar.db.sql.statement;

public interface DmlStatementVisitor {
  void visit(SelectStatement statement);

  void visit(InsertStatement statement);

  void visit(UpdateStatement statement);

  void visit(DeleteStatement statement);
}
