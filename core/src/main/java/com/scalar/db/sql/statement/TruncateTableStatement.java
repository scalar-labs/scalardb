package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TruncateTableStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;

  private TruncateTableStatement(String namespaceName, String tableName) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
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
        .add("tableName", tableName)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TruncateTableStatement)) {
      return false;
    }
    TruncateTableStatement that = (TruncateTableStatement) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName);
  }

  public static TruncateTableStatement of(String namespaceName, String tableName) {
    return new TruncateTableStatement(namespaceName, tableName);
  }
}
