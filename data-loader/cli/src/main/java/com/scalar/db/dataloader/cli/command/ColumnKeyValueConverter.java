package com.scalar.db.dataloader.cli.command;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import picocli.CommandLine;

/**
 * Converts a string representation of a key-value pair into a {@link ColumnKeyValue} object. The
 * string format should be "key=value".
 */
public class ColumnKeyValueConverter implements CommandLine.ITypeConverter<ColumnKeyValue> {

  /**
   * Converts a string representation of a key-value pair into a {@link ColumnKeyValue} object.
   *
   * @param keyValue the string representation of the key-value pair in the format "key=value"
   * @return a {@link ColumnKeyValue} object representing the key-value pair
   * @throws IllegalArgumentException if the input string is not in the expected format
   */
  @Override
  public ColumnKeyValue convert(String keyValue) {
    if (keyValue == null) {
      throw new IllegalArgumentException("Key-value cannot be null");
    }
    String[] parts = keyValue.split("=", 2);
    if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid key-value format: " + keyValue);
    }
    String columnName = parts[0].trim();
    String value = parts[1].trim();
    return new ColumnKeyValue(columnName, value);
  }
}
