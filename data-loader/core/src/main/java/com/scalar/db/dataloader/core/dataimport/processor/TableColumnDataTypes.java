package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that maintains a mapping of column data types for database tables.
 *
 * <p>This class provides functionality to store and retrieve data type information for table
 * columns in a database schema. It uses a nested map structure where the outer map keys are table
 * names and the inner map keys are column names.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * TableColumnDataTypes types = new TableColumnDataTypes();
 *
 * // Add column data types for a table
 * types.addColumnDataType("users", "id", DataType.INT);
 * types.addColumnDataType("users", "name", DataType.TEXT);
 *
 * // Retrieve data type for a specific column
 * DataType idType = types.getDataType("users", "id"); // Returns DataType.INT
 *
 * // Get all column data types for a table
 * Map<String, DataType> userColumns = types.getColumnDataTypes("users");
 * }</pre>
 */
public class TableColumnDataTypes {
  private final Map<String, Map<String, DataType>> dataTypesByColumnsByTable;

  /**
   * Constructs a new {@code TableColumnDataTypes} instance with an empty mapping. The internal
   * structure is initialized as an empty HashMap that will store table names as keys and
   * column-to-datatype mappings as values.
   */
  public TableColumnDataTypes() {
    this.dataTypesByColumnsByTable = new HashMap<>();
  }

  /**
   * Adds a data type for a specific column in a given table.
   *
   * <p>If the table doesn't exist in the mapping, a new entry is created automatically. If the
   * column already exists for the specified table, its data type will be updated with the new
   * value.
   *
   * @param tableName the name of the table
   * @param columnName the name of the column
   * @param dataType the data type associated with the column
   * @throws NullPointerException if any of the parameters is null
   */
  public void addColumnDataType(String tableName, String columnName, DataType dataType) {
    dataTypesByColumnsByTable
        .computeIfAbsent(tableName, key -> new HashMap<>())
        .put(columnName, dataType);
  }

  /**
   * Retrieves the data type of specific column in a given table.
   *
   * <p>This method performs a lookup in the internal mapping to find the data type associated with
   * the specified table and column combination.
   *
   * @param tableName the name of the table
   * @param columnName the name of the column
   * @return the {@link DataType} of the column, or {@code null} if either the table or the column
   *     is not found in the mapping
   * @throws NullPointerException if any of the parameters is null
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
   * <p>Returns a map containing all columns and their corresponding data types for the specified
   * table. The returned map is a direct reference to the internal map, so modifications to it will
   * affect the internal state.
   *
   * @param tableName the name of the table
   * @return a {@link Map} of column names to their respective {@link DataType}s, or {@code null} if
   *     the table does not exist in the mapping
   * @throws NullPointerException if tableName is null
   */
  public Map<String, DataType> getColumnDataTypes(String tableName) {
    return dataTypesByColumnsByTable.get(tableName);
  }
}
