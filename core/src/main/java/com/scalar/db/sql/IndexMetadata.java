package com.scalar.db.sql;

import javax.annotation.concurrent.Immutable;

@Immutable
public class IndexMetadata {

  private final String namespaceName;
  private final String tableName;
  private final String columnName;

  public IndexMetadata(String namespaceName, String tableName, String columnName) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.columnName = columnName;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }
}
