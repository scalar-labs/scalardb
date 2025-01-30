package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.io.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TableColumnDataTypesTest {

  TableColumnDataTypes tableColumnDataTypes;

  @Test
  void addColumnDataType_withValidData_shouldAddColumnDataType() {
    tableColumnDataTypes = new TableColumnDataTypes();
    tableColumnDataTypes.addColumnDataType("table", "id", DataType.BIGINT);
    tableColumnDataTypes.addColumnDataType("table", "name", DataType.TEXT);
    Assertions.assertEquals(
        DataType.BIGINT, tableColumnDataTypes.getColumnDataTypes("table").get("id"));
  }

  @Test
  void getDataType_withValidTableAndColumnName_shouldReturnCorrectDataType() {
    tableColumnDataTypes = new TableColumnDataTypes();
    tableColumnDataTypes.addColumnDataType("table", "id", DataType.BIGINT);
    tableColumnDataTypes.addColumnDataType("table", "name", DataType.TEXT);
    Assertions.assertEquals(DataType.TEXT, tableColumnDataTypes.getDataType("table", "name"));
  }

  @Test
  void getDataType_withInvalidTableAndColumnName_shouldReturnCorrectDataType() {
    tableColumnDataTypes = new TableColumnDataTypes();
    Assertions.assertNull(tableColumnDataTypes.getDataType("table", "name"));
  }
}
