package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.util.Objects;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("columnName", columnName)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IndexMetadata)) {
      return false;
    }
    IndexMetadata that = (IndexMetadata) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, columnName);
  }
}
