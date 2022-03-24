package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class InsertStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Assignment> assignments;

  private InsertStatement(
      String namespaceName, String tableName, ImmutableList<Assignment> assignments) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.assignments = Objects.requireNonNull(assignments);
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DmlStatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("assignments", assignments)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InsertStatement)) {
      return false;
    }
    InsertStatement that = (InsertStatement) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(assignments, that.assignments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, assignments);
  }

  public static InsertStatement of(
      String namespaceName, String tableName, ImmutableList<Assignment> assignments) {
    return new InsertStatement(namespaceName, tableName, assignments);
  }
}
