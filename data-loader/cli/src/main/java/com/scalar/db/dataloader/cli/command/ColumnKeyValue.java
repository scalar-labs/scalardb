package com.scalar.db.dataloader.cli.command;

/** Represents a key-value pair for a column and its corresponding value. */
public class ColumnKeyValue {
  private final String columnName;
  private final String columnValue;

  /**
   * Class constructor
   *
   * @param columnName the column name
   * @param columnValue the column value
   */
  public ColumnKeyValue(String columnName, String columnValue) {
    this.columnName = columnName;
    this.columnValue = columnValue;
  }

  /**
   * Gets the column name
   *
   * @return the column name
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Gets the column value
   *
   * @return the column value
   */
  public String getColumnValue() {
    return columnValue;
  }
}
