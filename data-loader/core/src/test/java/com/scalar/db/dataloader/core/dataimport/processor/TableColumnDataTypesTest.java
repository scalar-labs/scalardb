package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.io.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the TableColumnDataTypes class which manages data type mappings for table columns.
 */
class TableColumnDataTypesTest {

  TableColumnDataTypes tableColumnDataTypes;

  /**
   * Tests that column data types can be successfully added and retrieved for a table. Verifies that
   * the correct data type is returned for a specific column after adding multiple column
   * definitions.
   */
  @Test
  void addColumnDataType_withValidData_shouldAddColumnDataType() {
    tableColumnDataTypes = new TableColumnDataTypes();
    tableColumnDataTypes.addColumnDataType("table", "id", DataType.BIGINT);
    tableColumnDataTypes.addColumnDataType("table", "name", DataType.TEXT);
    Assertions.assertEquals(
        DataType.BIGINT, tableColumnDataTypes.getColumnDataTypes("table").get("id"));
  }

  /**
   * Tests the retrieval of a data type for a specific table and column combination. Verifies that
   * the correct data type is returned when the table and column exist in the mapping.
   */
  @Test
  void getDataType_withValidTableAndColumnName_shouldReturnCorrectDataType() {
    tableColumnDataTypes = new TableColumnDataTypes();
    tableColumnDataTypes.addColumnDataType("table", "id", DataType.BIGINT);
    tableColumnDataTypes.addColumnDataType("table", "name", DataType.TEXT);
    Assertions.assertEquals(DataType.TEXT, tableColumnDataTypes.getDataType("table", "name"));
  }

  /**
   * Tests the behavior when attempting to retrieve a data type for a non-existent table and column
   * combination. Verifies that null is returned when the requested mapping doesn't exist.
   */
  @Test
  void getDataType_withInvalidTableAndColumnName_shouldReturnCorrectDataType() {
    tableColumnDataTypes = new TableColumnDataTypes();
    Assertions.assertNull(tableColumnDataTypes.getDataType("table", "name"));
  }
}
