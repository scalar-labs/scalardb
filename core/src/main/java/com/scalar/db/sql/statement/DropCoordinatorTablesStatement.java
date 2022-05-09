package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DropCoordinatorTablesStatement implements DdlStatement {

  public final boolean ifExist;

  private DropCoordinatorTablesStatement(boolean ifExist) {
    this.ifExist = ifExist;
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
    return MoreObjects.toStringHelper(this).add("ifExist", ifExist).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DropCoordinatorTablesStatement)) {
      return false;
    }
    DropCoordinatorTablesStatement that = (DropCoordinatorTablesStatement) o;
    return ifExist == that.ifExist;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ifExist);
  }

  public static DropCoordinatorTablesStatement of(boolean ifExist) {
    return new DropCoordinatorTablesStatement(ifExist);
  }
}
