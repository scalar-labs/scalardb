package com.scalar.db.sql.builder;

import java.util.Arrays;
import java.util.List;

public final class StatementBuilder {

  private StatementBuilder() {}

  public static CreateNamespaceStatementBuilder.Start createNamespace(String namespaceName) {
    return new CreateNamespaceStatementBuilder.Start(namespaceName);
  }

  public static DropNamespaceStatementBuilder.Start dropNamespace(String namespaceName) {
    return new DropNamespaceStatementBuilder.Start(namespaceName);
  }

  public static CreateTableStatementBuilder.Start createTable(
      String namespaceName, String tableName) {
    return new CreateTableStatementBuilder.Start(namespaceName, tableName);
  }

  public static DropTableStatementBuilder.Start dropTable(String namespaceName, String tableName) {
    return new DropTableStatementBuilder.Start(namespaceName, tableName);
  }

  public static TruncateTableStatementBuilder.Start truncateTable(
      String namespaceName, String tableName) {
    return new TruncateTableStatementBuilder.Start(namespaceName, tableName);
  }

  public static CreateCoordinatorTableStatementBuilder.Start createCoordinatorTable() {
    return new CreateCoordinatorTableStatementBuilder.Start();
  }

  public static DropCoordinatorTableStatementBuilder.Start dropCoordinatorTable() {
    return new DropCoordinatorTableStatementBuilder.Start();
  }

  public static TruncateCoordinatorTableStatementBuilder.Start truncateCoordinatorTable() {
    return new TruncateCoordinatorTableStatementBuilder.Start();
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
