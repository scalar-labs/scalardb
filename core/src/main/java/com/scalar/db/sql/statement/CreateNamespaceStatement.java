package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CreateNamespaceStatement implements DdlStatement {

  public final String namespaceName;
  public final boolean ifNotExists;
  public final ImmutableMap<String, String> options;

  private CreateNamespaceStatement(
      String namespaceName, boolean ifNotExists, ImmutableMap<String, String> options) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.ifNotExists = ifNotExists;
    this.options = Objects.requireNonNull(options);
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
        .add("ifNotExists", ifNotExists)
        .add("options", options)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateNamespaceStatement)) {
      return false;
    }
    CreateNamespaceStatement that = (CreateNamespaceStatement) o;
    return ifNotExists == that.ifNotExists
        && Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, ifNotExists, options);
  }

  public static CreateNamespaceStatement of(
      String namespaceName, boolean ifNotExists, ImmutableMap<String, String> options) {
    return new CreateNamespaceStatement(namespaceName, ifNotExists, options);
  }
}
