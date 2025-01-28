package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import picocli.CommandLine;

public class ScanOrderingConverter implements CommandLine.ITypeConverter<Scan.Ordering> {
  @Override
  public Scan.Ordering convert(String value) throws Exception {
    String[] parts = value.split("=");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid column order format: " + value);
    }
    String columnName = parts[0].trim();
    Scan.Ordering.Order sortOrder = Scan.Ordering.Order.valueOf(parts[1].trim().toUpperCase());
    return new Scan.Ordering(columnName, sortOrder);
  }
}
