package com.scalar.db.sql.statement;

public interface DdlStatementVisitor<R, C> {
  R visit(CreateNamespaceStatement statement, C context);

  R visit(CreateTableStatement statement, C context);

  R visit(DropNamespaceStatement statement, C context);

  R visit(DropTableStatement statement, C context);

  R visit(TruncateTableStatement statement, C context);

  R visit(CreateCoordinatorTableStatement statement, C context);

  R visit(DropCoordinatorTableStatement statement, C context);

  R visit(TruncateCoordinatorTableStatement statement, C context);

  R visit(CreateIndexStatement statement, C context);

  R visit(DropIndexStatement statement, C context);
}
