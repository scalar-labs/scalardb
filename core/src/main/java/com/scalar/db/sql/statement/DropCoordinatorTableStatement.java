package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DropCoordinatorTableStatement implements DdlStatement {

  public final boolean ifExists;

  private DropCoordinatorTableStatement(boolean ifExists) {
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
    return MoreObjects.toStringHelper(this).add("ifExists", ifExists).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DropCoordinatorTableStatement)) {
      return false;
    }
    DropCoordinatorTableStatement that = (DropCoordinatorTableStatement) o;
    return ifExists == that.ifExists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ifExists);
  }

  public static DropCoordinatorTableStatement of(boolean ifExists) {
    return new DropCoordinatorTableStatement(ifExists);
  }
}
