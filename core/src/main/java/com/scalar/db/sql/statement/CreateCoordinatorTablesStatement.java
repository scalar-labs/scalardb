package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CreateCoordinatorTablesStatement implements DdlStatement {

  public final boolean ifNotExist;
  public final ImmutableMap<String, String> options;

  private CreateCoordinatorTablesStatement(
      boolean ifNotExist, ImmutableMap<String, String> options) {
    this.ifNotExist = ifNotExist;
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
        .add("ifNotExist", ifNotExist)
        .add("options", options)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateCoordinatorTablesStatement)) {
      return false;
    }
    CreateCoordinatorTablesStatement that = (CreateCoordinatorTablesStatement) o;
    return ifNotExist == that.ifNotExist && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ifNotExist, options);
  }

  public static CreateCoordinatorTablesStatement of(
      boolean ifNotExist, ImmutableMap<String, String> options) {
    return new CreateCoordinatorTablesStatement(ifNotExist, options);
  }
}
