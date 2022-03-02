package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Condition;
import com.scalar.db.sql.Ordering;

public class SelectStatement implements Statement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<String> projectedColumnNames;
  public final ImmutableList<Condition> whereConditions;
  public final ImmutableList<Ordering> orderings;
  public final int limit;

  public SelectStatement(
      String namespaceName,
      String tableName,
      ImmutableList<String> projectedColumnNames,
      ImmutableList<Condition> whereConditions,
      ImmutableList<Ordering> orderings,
      int limit) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.projectedColumnNames = projectedColumnNames;
    this.whereConditions = whereConditions;
    this.orderings = orderings;
    this.limit = limit;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }
}
