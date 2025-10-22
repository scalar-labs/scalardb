package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.cli.util.CommandLineInputUtils;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import picocli.CommandLine;

/**
 * A {@link picocli.CommandLine.ITypeConverter} implementation that converts a comma-separated
 * string of column-order pairs into a list of {@link com.scalar.db.api.Scan.Ordering} objects.
 *
 * <p>This converter is used to parse CLI arguments for scan ordering in ScalarDB-based
 * applications. The input string must contain one or more key-value pairs in the format {@code
 * column=order}, separated by commas. The {@code order} must be a valid {@link
 * com.scalar.db.api.Scan.Ordering.Order} enum value, such as {@code ASC} or {@code DESC}
 * (case-insensitive).
 *
 * <p>Example input: {@code "name=asc,age=desc"} results in a list containing {@code
 * Scan.Ordering.asc("name")} and {@code Scan.Ordering.desc("age")}.
 *
 * <p>Invalid formats or unrecognized order values will result in an {@link
 * IllegalArgumentException}.
 */
public class ScanOrderingConverter implements CommandLine.ITypeConverter<List<Scan.Ordering>> {
  /**
   * Converts a comma-separated string of key-value pairs into a list of {@link
   * com.scalar.db.api.Scan.Ordering} objects. Each pair must be in the format "column=order", where
   * "order" is a valid enum value of {@link com.scalar.db.api.Scan.Ordering} (e.g., ASC or DESC,
   * case-insensitive).
   *
   * @param value the comma-separated key-value string to convert
   * @return a list of {@link com.scalar.db.api.Scan.Ordering} objects constructed from the input
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
              return sortOrder == Scan.Ordering.Order.ASC
                  ? Scan.Ordering.asc(columnName)
                  : Scan.Ordering.desc(columnName);
            })
        .collect(Collectors.toList());
  }
}
