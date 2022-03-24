package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import javax.annotation.concurrent.Immutable;

@Immutable
public class InsertStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Assignment> assignments;

  public InsertStatement(
      String namespaceName, String tableName, ImmutableList<Assignment> assignments) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.assignments = assignments;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DmlStatementVisitor visitor) {
    visitor.visit(this);
  }
}
