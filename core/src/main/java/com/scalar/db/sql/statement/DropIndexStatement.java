package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DropIndexStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final String columnName;
  public final boolean ifExists;

  private DropIndexStatement(
      String namespaceName, String tableName, String columnName, boolean ifExists) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.columnName = Objects.requireNonNull(columnName);
    this.ifExists = ifExists;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public <R, C> R accept(DdlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("columnName", columnName)
        .add("ifExists", ifExists)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DropIndexStatement)) {
      return false;
    }
    DropIndexStatement that = (DropIndexStatement) o;
    return ifExists == that.ifExists
        && Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, columnName, ifExists);
  }

  public static DropIndexStatement of(
      String namespaceName, String tableName, String columnName, boolean ifExists) {
    return new DropIndexStatement(namespaceName, tableName, columnName, ifExists);
  }
}
