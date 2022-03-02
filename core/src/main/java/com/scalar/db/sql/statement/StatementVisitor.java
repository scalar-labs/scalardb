package com.scalar.db.sql.statement;

public interface StatementVisitor {

  void visit(CreateNamespaceStatement statement);

  void visit(CreateTableStatement statement);

  void visit(DropNamespaceStatement statement);

  void visit(DropTableStatement statement);

  void visit(TruncateTableStatement statement);

  void visit(SelectStatement statement);

  void visit(InsertStatement statement);

  void visit(UpdateStatement statement);

  void visit(DeleteStatement statement);

  void visit(BatchStatement statement);
}
