package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TruncateCoordinatorTablesStatement implements DdlStatement {

  private static final TruncateCoordinatorTablesStatement INSTANCE =
      new TruncateCoordinatorTablesStatement();

  private TruncateCoordinatorTablesStatement() {}

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

  public static TruncateCoordinatorTablesStatement of() {
    return INSTANCE;
  }
}
