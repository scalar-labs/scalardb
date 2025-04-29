package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import picocli.CommandLine;

/**
 * Converts a string representation of a key-value pair into a {@link ColumnKeyValue} object. The
 * string format should be "key=value".
 */
public class SingleColumnKeyValueConverter implements CommandLine.ITypeConverter<ColumnKeyValue> {

  /**
   * Converts a string representation of a key-value pair into a {@link ColumnKeyValue} object.
   *
   * @param keyValue the string representation of the key-value pair in the format "key=value"
   * @return a {@link ColumnKeyValue} object representing the key-value pair
   * @throws IllegalArgumentException if the input string is not in the expected format
   */
  @Override
  public ColumnKeyValue convert(String keyValue) {
    if (keyValue == null || keyValue.trim().isEmpty()) {
      throw new IllegalArgumentException("Key-value cannot be null or empty");
    }
    return parseKeyValue(keyValue);
  }

  /**
   * Parses a single key-value pair from a string in the format "key=value".
   *
   * @param keyValue the key-value string to parse
   * @return a {@link ColumnKeyValue} object representing the parsed key-value pair
   * @throws IllegalArgumentException if the input is not in the expected format
   */
  private ColumnKeyValue parseKeyValue(String keyValue) {
    String[] parts = keyValue.split("=", 2);

    if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid key-value format: " + keyValue);
    }

    return new ColumnKeyValue(parts[0].trim(), parts[1].trim());
  }
}
