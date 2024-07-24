package com.scalar.db.dataloader.core;

import lombok.Builder;
import lombok.Value;

/** Represents a column in a table. */
@Value
@Builder
public class ColumnInfo {
  String namespace;
  String tableName;
  String columnName;
}
