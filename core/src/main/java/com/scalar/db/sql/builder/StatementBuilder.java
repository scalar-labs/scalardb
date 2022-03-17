package com.scalar.db.sql.builder;

import java.util.Arrays;
import java.util.List;

public final class StatementBuilder {

  private StatementBuilder() {}

  public static CreateNamespaceStatementBuilder createNamespace(String namespaceName) {
    return new CreateNamespaceStatementBuilder(namespaceName);
  }

  public static DropNamespaceStatementBuilder dropNamespace(String namespaceName) {
    return new DropNamespaceStatementBuilder(namespaceName);
  }

  public static CreateTableStatementBuilder.Start createTable(
      String namespaceName, String tableName) {
    return new CreateTableStatementBuilder.Start(namespaceName, tableName);
  }

  public static DropTableStatementBuilder dropTable(String namespaceName, String tableName) {
    return new DropTableStatementBuilder(namespaceName, tableName);
  }

  public static TruncateTableStatementBuilder truncateTable(
      String namespaceName, String tableName) {
    return new TruncateTableStatementBuilder(namespaceName, tableName);
  }

  public static CreateCoordinatorTableStatementBuilder createCoordinatorTable() {
    return new CreateCoordinatorTableStatementBuilder();
  }

  public static DropCoordinatorTableStatementBuilder dropCoordinatorTable() {
    return new DropCoordinatorTableStatementBuilder();
  }

  public static TruncateCoordinatorTableStatementBuilder truncateCoordinatorTable() {
    return new TruncateCoordinatorTableStatementBuilder();
  }

  public static CreateIndexStatementBuilder.Start createIndex() {
    return new CreateIndexStatementBuilder.Start();
  }

  public static DropIndexStatementBuilder.Start dropIndex() {
    return new DropIndexStatementBuilder.Start();
  }

  public static SelectStatementBuilder.Start select(String... projectedColumnNames) {
    return new SelectStatementBuilder.Start(Arrays.asList(projectedColumnNames));
  }

  public static SelectStatementBuilder.Start select(List<String> projectedColumnNames) {
    return new SelectStatementBuilder.Start(projectedColumnNames);
  }

  public static InsertStatementBuilder.Start insertInto(String namespaceName, String tableName) {
    return new InsertStatementBuilder.Start(namespaceName, tableName);
  }

  public static UpdateStatementBuilder.Start update(String namespaceName, String tableName) {
    return new UpdateStatementBuilder.Start(namespaceName, tableName);
  }

  public static DeleteStatementBuilder.Start deleteFrom(String namespaceName, String tableName) {
    return new DeleteStatementBuilder.Start(namespaceName, tableName);
  }
}
