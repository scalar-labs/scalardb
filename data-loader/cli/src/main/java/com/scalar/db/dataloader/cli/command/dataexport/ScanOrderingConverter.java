package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

public class ScanOrderingConverter implements CommandLine.ITypeConverter<List<Scan.Ordering>> {
  @Override
  public List<Scan.Ordering> convert(String value) {
    String[] parts = value.split(",");
    List<Scan.Ordering> scanOrderList = new ArrayList<>();
    for (String part : parts) {
      String[] data = part.split("=");

      if (data.length != 2) {
        throw new IllegalArgumentException("Invalid column order format: " + value);
      }
      String columnName = data[0].trim();
      Scan.Ordering.Order sortOrder = Scan.Ordering.Order.valueOf(data[1].trim().toUpperCase());
      scanOrderList.add(new Scan.Ordering(columnName, sortOrder));
    }
    return scanOrderList;
  }
}
