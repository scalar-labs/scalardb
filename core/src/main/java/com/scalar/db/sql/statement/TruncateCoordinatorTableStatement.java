package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TruncateCoordinatorTableStatement implements DdlStatement {

  private static final TruncateCoordinatorTableStatement INSTANCE =
      new TruncateCoordinatorTableStatement();

  private TruncateCoordinatorTableStatement() {}

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
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public static TruncateCoordinatorTableStatement of() {
    return INSTANCE;
  }
}
