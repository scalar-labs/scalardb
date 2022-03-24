package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CreateIndexStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final String columnName;
  public final boolean ifNotExists;
  public final ImmutableMap<String, String> options;

  public CreateIndexStatement(
      String namespaceName,
      String tableName,
      String columnName,
      boolean ifNotExists,
      ImmutableMap<String, String> options) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.columnName = Objects.requireNonNull(columnName);
    this.ifNotExists = ifNotExists;
    this.options = Objects.requireNonNull(options);
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
        .add("columnName", columnName)
        .add("ifNotExists", ifNotExists)
        .add("options", options)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateIndexStatement)) {
      return false;
    }
    CreateIndexStatement that = (CreateIndexStatement) o;
    return ifNotExists == that.ifNotExists
        && Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName)
        && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, columnName, ifNotExists, options);
  }
}
