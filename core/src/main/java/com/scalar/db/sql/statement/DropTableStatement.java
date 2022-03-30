package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DropTableStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final boolean ifExists;

  private DropTableStatement(String namespaceName, String tableName, boolean ifExists) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
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
        .add("ifExists", ifExists)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DropTableStatement)) {
      return false;
    }
    DropTableStatement that = (DropTableStatement) o;
    return ifExists == that.ifExists
        && Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, ifExists);
  }

  public static DropTableStatement of(String namespaceName, String tableName, boolean ifExists) {
    return new DropTableStatement(namespaceName, tableName, ifExists);
  }
}
