package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DropCoordinatorTableStatement implements DdlStatement {

  public final boolean ifExists;

  public DropCoordinatorTableStatement(boolean ifExists) {
    this.ifExists = ifExists;
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
}
