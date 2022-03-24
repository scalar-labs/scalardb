package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DropNamespaceStatement implements DdlStatement {

  public final String namespaceName;
  public final boolean ifExists;
  public final boolean cascade;

  public DropNamespaceStatement(String namespaceName, boolean ifExists, boolean cascade) {
    this.namespaceName = namespaceName;
    this.ifExists = ifExists;
    this.cascade = cascade;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DdlStatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("ifExists", ifExists)
        .add("cascade", cascade)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DropNamespaceStatement)) {
      return false;
    }
    DropNamespaceStatement that = (DropNamespaceStatement) o;
    return ifExists == that.ifExists
        && cascade == that.cascade
        && Objects.equals(namespaceName, that.namespaceName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, ifExists, cascade);
  }
}
