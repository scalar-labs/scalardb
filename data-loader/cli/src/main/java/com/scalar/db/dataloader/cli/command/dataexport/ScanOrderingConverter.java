package com.scalar.db.dataloader.cli.command.dataexport;

import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.splitByDelimiter;

import com.scalar.db.api.Scan;
import com.scalar.db.common.error.CoreError;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

public class ScanOrderingConverter implements CommandLine.ITypeConverter<List<Scan.Ordering>> {
  @Override
  public List<Scan.Ordering> convert(String value) {
    String[] parts = splitByDelimiter(value, ",", 0);
    List<Scan.Ordering> scanOrderList = new ArrayList<>();
    for (String part : parts) {
      String[] data = splitByDelimiter(part, "=", 2);
      if (data.length != 2) {
        throw new IllegalArgumentException(
            CoreError.DATA_LOADER_INVALID_COLUMN_ORDER_FORMAT.buildMessage(value));
      }
      String columnName = data[0].trim();
      Scan.Ordering.Order sortOrder = Scan.Ordering.Order.valueOf(data[1].trim().toUpperCase());
      scanOrderList.add(new Scan.Ordering(columnName, sortOrder));
    }
    return scanOrderList;
  }
}
