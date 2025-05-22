package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.cli.util.CommandLineInputUtils;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import picocli.CommandLine;

public class ScanOrderingConverter implements CommandLine.ITypeConverter<List<Scan.Ordering>> {
  /**
   * Converts a comma-separated string of key-value pairs into a list of {@link Scan.Ordering}
   * objects. Each pair must be in the format "column=order", where "order" is a valid enum value of
   * {@link Scan.Ordering.Order} (e.g., ASC or DESC, case-insensitive).
   *
   * @param value the comma-separated key-value string to convert
   * @return a list of {@link Scan.Ordering} objects constructed from the input
   * @throws IllegalArgumentException if parsing fails due to invalid format or enum value
   */
  @Override
  public List<Scan.Ordering> convert(String value) {
    return Arrays.stream(CommandLineInputUtils.splitByDelimiter(value, ",", 0))
        .map(CommandLineInputUtils::parseKeyValue)
        .map(
            entry -> {
              String columnName = entry.getKey();
              Scan.Ordering.Order sortOrder =
                  Scan.Ordering.Order.valueOf(entry.getValue().trim().toUpperCase());
              return new Scan.Ordering(columnName, sortOrder);
            })
        .collect(Collectors.toList());
  }
}
