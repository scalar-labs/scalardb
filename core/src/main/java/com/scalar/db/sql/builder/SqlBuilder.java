package com.scalar.db.sql.builder;

import java.util.Arrays;

public final class SqlBuilder {

  private SqlBuilder() {}

  public static CreateNamespaceBuilder createNamespace(String namespaceName) {
    return new CreateNamespaceBuilder(namespaceName);
  }

  public static DropNamespaceBuilder dropNamespace(String namespaceName) {
    return new DropNamespaceBuilder(namespaceName);
  }

  public static CreateTableBuilder.Start createTable(String namespaceName, String tableName) {
    return new CreateTableBuilder.Start(namespaceName, tableName);
  }

  public static DropTableBuilder dropTable(String namespaceName, String tableName) {
    return new DropTableBuilder(namespaceName, tableName);
  }

  public static TruncateTableBuilder truncateTable(String namespaceName, String tableName) {
    return new TruncateTableBuilder(namespaceName, tableName);
  }

  public static SelectBuilder.Start select(String... projectedColumnNames) {
    return new SelectBuilder.Start(Arrays.asList(projectedColumnNames));
  }

  public static InsertBuilder.Start insertInto(String namespaceName, String tableName) {
    return new InsertBuilder.Start(namespaceName, tableName);
  }

  public static UpdateBuilder.Start update(String namespaceName, String tableName) {
    return new UpdateBuilder.Start(namespaceName, tableName);
  }

  public static DeleteBuilder.Start deleteFrom(String namespaceName, String tableName) {
    return new DeleteBuilder.Start(namespaceName, tableName);
  }

  public static BatchBuilder batch() {
    return new BatchBuilder();
  }
}
