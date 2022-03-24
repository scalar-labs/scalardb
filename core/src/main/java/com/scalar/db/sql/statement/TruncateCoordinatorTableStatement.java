package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TruncateCoordinatorTableStatement implements DdlStatement {

  private static final TruncateCoordinatorTableStatement INSTANCE =
      new TruncateCoordinatorTableStatement();

  private TruncateCoordinatorTableStatement() {}

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
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof TruncateCoordinatorTableStatement;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public static TruncateCoordinatorTableStatement of() {
    return INSTANCE;
  }
}
