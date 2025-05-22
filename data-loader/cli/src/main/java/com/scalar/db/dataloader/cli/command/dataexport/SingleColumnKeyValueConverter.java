package com.scalar.db.dataloader.cli.command.dataexport;

import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.parseKeyValue;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import java.util.Map;
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
    Map.Entry<String, String> data = parseKeyValue(keyValue);
    return new ColumnKeyValue(data.getKey(), data.getValue());
  }
}
