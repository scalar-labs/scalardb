package com.scalar.db.dataloader.core;

import lombok.Builder;
import lombok.Value;

/**
 * Represents a column in a database table.
 *
 * <p>This class holds the metadata for a column, including the namespace (schema), table name, and
 * the column name within the table.
 */
@SuppressWarnings("SameNameButDifferent")
@Value
@Builder
public class ColumnInfo {

  /** The namespace (schema) where the table is located. */
  String namespace;

  /** The name of the table where the column resides. */
  String tableName;

  /** The name of the column in the table. */
  String columnName;
}
