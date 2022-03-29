package com.scalar.db.sql.statement;

public interface DmlStatementVisitor<R, C> {
  R visit(SelectStatement statement, C context);

  R visit(InsertStatement statement, C context);

  R visit(UpdateStatement statement, C context);

  R visit(DeleteStatement statement, C context);
}
