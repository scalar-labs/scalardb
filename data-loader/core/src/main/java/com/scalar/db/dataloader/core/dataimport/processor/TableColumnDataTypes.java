package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Map;

public class TableColumnDataTypes {
  private final Map<String, Map<String, DataType>> dataTypesByColumnsByTable;

  public TableColumnDataTypes() {
    this.dataTypesByColumnsByTable = new HashMap<>();
  }

  public void addColumnDataType(String tableName, String columnName, DataType dataType) {
    dataTypesByColumnsByTable
        .computeIfAbsent(tableName, key -> new HashMap<>())
        .put(columnName, dataType);
  }

  public DataType getDataType(String tableName, String columnName) {
    Map<String, DataType> columnDataTypes = dataTypesByColumnsByTable.get(tableName);
    if (columnDataTypes != null) {
      return columnDataTypes.get(columnName);
    }
    return null;
  }

  public Map<String, DataType> getColumnDataTypes(String tableName) {
    return dataTypesByColumnsByTable.get(tableName);
  }
}
