package com.scalar.db.sql;

public class ColumnMetadata {

  private final String namespaceName;
  private final String tableName;
  private final String name;
  private final DataType dataType;

  public ColumnMetadata(String namespaceName, String tableName, String name, DataType dataType) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.name = name;
    this.dataType = dataType;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getName() {
    return name;
  }

  public DataType getDataType() {
    return dataType;
  }
}
