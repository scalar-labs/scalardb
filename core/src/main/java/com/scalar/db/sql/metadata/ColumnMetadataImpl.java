package com.scalar.db.sql.metadata;

import com.google.common.base.MoreObjects;
import com.scalar.db.sql.DataType;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ColumnMetadataImpl implements ColumnMetadata {

  private final String namespaceName;
  private final String tableName;
  private final String name;
  private final DataType dataType;

  ColumnMetadataImpl(String namespaceName, String tableName, String name, DataType dataType) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.name = Objects.requireNonNull(name);
    this.dataType = Objects.requireNonNull(dataType);
  }

  @Override
  public String getNamespaceName() {
    return namespaceName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("name", name)
        .add("dataType", dataType)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnMetadataImpl)) {
      return false;
    }
    ColumnMetadataImpl that = (ColumnMetadataImpl) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(name, that.name)
        && dataType == that.dataType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, tableName, name, dataType);
  }
}
