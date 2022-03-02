package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.Condition;

public class UpdateStatement implements Statement, BatchableStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Assignment> assignments;
  public final ImmutableList<Condition> whereConditions;
  public final ImmutableList<Condition> ifConditions;
  public final boolean ifExists;

  public UpdateStatement(
      String namespaceName,
      String tableName,
      ImmutableList<Assignment> assignments,
      ImmutableList<Condition> whereConditions,
      ImmutableList<Condition> ifConditions,
      boolean ifExists) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.assignments = assignments;
    this.whereConditions = whereConditions;
    this.ifConditions = ifConditions;
    this.ifExists = ifExists;
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
