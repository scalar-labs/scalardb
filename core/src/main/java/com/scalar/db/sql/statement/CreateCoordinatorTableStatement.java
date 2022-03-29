package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CreateCoordinatorTableStatement implements DdlStatement {

  public final boolean ifNotExists;
  public final ImmutableMap<String, String> options;

  private CreateCoordinatorTableStatement(
      boolean ifNotExists, ImmutableMap<String, String> options) {
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
        .add("ifNotExists", ifNotExists)
        .add("options", options)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateCoordinatorTableStatement)) {
      return false;
    }
    CreateCoordinatorTableStatement that = (CreateCoordinatorTableStatement) o;
    return ifNotExists == that.ifNotExists && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ifNotExists, options);
  }

  public static CreateCoordinatorTableStatement of(
      boolean ifNotExists, ImmutableMap<String, String> options) {
    return new CreateCoordinatorTableStatement(ifNotExists, options);
  }
}
