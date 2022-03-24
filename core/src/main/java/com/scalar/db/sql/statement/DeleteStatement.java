package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Predicate;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DeleteStatement implements DmlStatement {

  public final String namespaceName;
  public final String tableName;
  public final ImmutableList<Predicate> predicates;

  private DeleteStatement(
      String namespaceName, String tableName, ImmutableList<Predicate> predicates) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.predicates = Objects.requireNonNull(predicates);
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
        .add("predicates", predicates)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeleteStatement)) {
      return false;
    }
    DeleteStatement that = (DeleteStatement) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(predicates, that.predicates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, predicates);
  }

  public static DeleteStatement of(
      String namespaceName, String tableName, ImmutableList<Predicate> predicates) {
    return new DeleteStatement(namespaceName, tableName, predicates);
  }
}
