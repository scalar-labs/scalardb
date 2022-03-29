package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.Predicate;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class UpdateStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Assignment> assignments;
  public final ImmutableList<Predicate> predicates;

  private UpdateStatement(
      String namespaceName,
      String tableName,
      ImmutableList<Assignment> assignments,
      ImmutableList<Predicate> predicates) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.assignments = Objects.requireNonNull(assignments);
    this.predicates = Objects.requireNonNull(predicates);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public <R, C> R accept(DmlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("assignments", assignments)
        .add("predicates", predicates)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UpdateStatement)) {
      return false;
    }
    UpdateStatement that = (UpdateStatement) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(assignments, that.assignments)
        && Objects.equals(predicates, that.predicates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, assignments, predicates);
  }

  public static UpdateStatement of(
      String namespaceName,
      String tableName,
      ImmutableList<Assignment> assignments,
      ImmutableList<Predicate> predicates) {
    return new UpdateStatement(namespaceName, tableName, assignments, predicates);
  }
}
