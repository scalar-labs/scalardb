package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that maintains a mapping of column data types for database tables.
 *
 * <p>This class allows storing and retrieving data types for specific columns in a given table.
 */
public class TableColumnDataTypes {
  private final Map<String, Map<String, DataType>> dataTypesByColumnsByTable;

  /** Constructs a new {@code TableColumnDataTypes} instance with an empty mapping. */
  public TableColumnDataTypes() {
    this.dataTypesByColumnsByTable = new HashMap<>();
  }

  /**
   * Adds a data type for a specific column in a given table.
   *
   * @param tableName the name of the table
   * @param columnName the name of the column
   * @param dataType the data type associated with the column
   */
  public void addColumnDataType(String tableName, String columnName, DataType dataType) {
    dataTypesByColumnsByTable
        .computeIfAbsent(tableName, key -> new HashMap<>())
        .put(columnName, dataType);
  }

  /**
   * Retrieves the data type of specific column in a given table.
   *
   * @param tableName the name of the table
   * @param columnName the name of the column
   * @return the {@link DataType} of the column, or {@code null} if not found
   */
  public DataType getDataType(String tableName, String columnName) {
    Map<String, DataType> columnDataTypes = dataTypesByColumnsByTable.get(tableName);
    if (columnDataTypes != null) {
      return columnDataTypes.get(columnName);
    }
    return null;
  }

  /**
   * Retrieves all column data types for a given table.
   *
   * @param tableName the name of the table
   * @return a {@link Map} of column names to their respective {@link DataType}s, or {@code null} if
   *     the table does not exist
   */
  public Map<String, DataType> getColumnDataTypes(String tableName) {
    return dataTypesByColumnsByTable.get(tableName);
  }
}
