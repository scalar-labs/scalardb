package com.scalar.db.sql.statement;

public interface DdlStatementVisitor {
  void visit(CreateNamespaceStatement statement);

  void visit(CreateTableStatement statement);

  void visit(DropNamespaceStatement statement);

  void visit(DropTableStatement statement);

  void visit(TruncateTableStatement statement);

  void visit(CreateCoordinatorTableStatement statement);

  void visit(DropCoordinatorTableStatement statement);

  void visit(TruncateCoordinatorTableStatement statement);

  void visit(CreateIndexStatement statement);

  void visit(DropIndexStatement statement);
}
