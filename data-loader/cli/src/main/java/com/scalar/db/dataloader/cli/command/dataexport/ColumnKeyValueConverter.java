package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

public class ColumnKeyValueConverter implements CommandLine.ITypeConverter<List<ColumnKeyValue>> {

  @Override
  public List<ColumnKeyValue> convert(String keyValue) {
    List<ColumnKeyValue> columnKeyValueList = new ArrayList<>();
    String[] columnValues = keyValue.split(",");
    for (int i = 0; i < columnValues.length; i++) {
      String[] parts = columnValues[i].split("=");

      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid key format: " + keyValue);
      }
      String columnName = parts[0].trim();
      String value = parts[1].trim();
      columnKeyValueList.add(new ColumnKeyValue(columnName, value));
    }
    return columnKeyValueList;
  }
}
