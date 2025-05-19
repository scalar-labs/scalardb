package com.scalar.db.dataloader.cli.command.dataexport;

import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.parseKeyValue;
import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.splitByDelimiter;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

/**
 * Converts a string representation of multiple key-value pairs into a list of {@link
 * ColumnKeyValue} objects.
 *
 * <p>The expected format for the input string is: {@code key1=value1,key2=value2,...}. Each
 * key-value pair should be separated by a comma, and each pair must follow the "key=value" format.
 *
 * <p>Example usage:
 *
 * <pre>
 *   MultiColumnKeyValueConverter converter = new MultiColumnKeyValueConverter();
 *   List&lt;ColumnKeyValue&gt; result = converter.convert("name=John,age=30,city=New York");
 * </pre>
 */
public class MultiColumnKeyValueConverter
    implements CommandLine.ITypeConverter<List<ColumnKeyValue>> {

  /**
   * Converts a comma-separated string of key-value pairs into a list of {@link ColumnKeyValue}
   * objects.
   *
   * @param keyValue the input string in the format {@code key1=value1,key2=value2,...}
   * @return a list of {@link ColumnKeyValue} objects representing the parsed key-value pairs
   * @throws IllegalArgumentException if the input is null, empty, or contains invalid formatting
   */
  @Override
  public List<ColumnKeyValue> convert(String keyValue) {
    if (keyValue == null || keyValue.trim().isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.DATA_LOADER_NULL_OR_EMPTY_KEY_VALUE_INPUT.buildMessage());
    }

    List<ColumnKeyValue> columnKeyValueList = new ArrayList<>();
    String[] columnValues = splitByDelimiter(keyValue, ",", 0);

    for (String columnValue : columnValues) {
      columnKeyValueList.add(parseKeyValue(columnValue));
    }

    return columnKeyValueList;
  }
}
