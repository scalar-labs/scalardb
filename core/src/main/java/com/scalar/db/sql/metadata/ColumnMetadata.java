package com.scalar.db.sql.metadata;

import com.scalar.db.sql.DataType;

public interface ColumnMetadata {

  String getNamespaceName();

  String getTableName();

  String getName();

  DataType getDataType();
}
