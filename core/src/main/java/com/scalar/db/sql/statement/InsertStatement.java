package com.scalar.db.sql.statement;

import com.scalar.db.sql.Assignment;
import java.util.List;

public class InsertStatement implements Statement, BatchableStatement {

  public final String namespaceName;
  public final String tableName;
  public final List<Assignment> assignments;
  public final boolean ifNotExists;

  public InsertStatement(
      String namespaceName, String tableName, List<Assignment> assignments, boolean ifNotExists) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.assignments = assignments;
    this.ifNotExists = ifNotExists;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(BatchableStatementVisitor visitor) {
    visitor.visit(this);
  }
}
