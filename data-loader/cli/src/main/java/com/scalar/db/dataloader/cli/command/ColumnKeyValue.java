package com.scalar.db.dataloader.cli.command;

import lombok.Value;

/** Represents a key-value pair for a column and its corresponding value. */
@Value
public class ColumnKeyValue {
  String columnName;
  String value;
}
