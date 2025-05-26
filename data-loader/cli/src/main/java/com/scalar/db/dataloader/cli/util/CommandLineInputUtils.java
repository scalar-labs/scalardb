package com.scalar.db.dataloader.cli.util;

import com.scalar.db.common.error.CoreError;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class CommandLineInputUtils {

  /**
   * Parses a single key-value pair from a string in the format "key=value".
   *
   * @param keyValue the key-value string to parse
   * @return a {@link Map.Entry} representing the parsed key-value pair
   * @throws IllegalArgumentException if the input is null, empty, or not in the expected format
   */
  public static Map.Entry<String, String> parseKeyValue(String keyValue) {
    if (StringUtils.isBlank(keyValue)) {
      throw new IllegalArgumentException(
          CoreError.DATA_LOADER_NULL_OR_EMPTY_KEY_VALUE_INPUT.buildMessage());
    }

    String[] parts = splitByDelimiter(keyValue, "=", 2);

    if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.DATA_LOADER_INVALID_KEY_VALUE_INPUT.buildMessage(keyValue));
    }
    return new AbstractMap.SimpleEntry<>(parts[0].trim(), parts[1].trim());
  }

  /**
   * Splits a string based on the provided delimiter.
   *
   * @param value the string to split
   * @param delimiter the delimiter to use
   * @param limit the maximum number of elements in the result (same behavior as String.split() with
   *     limit)
   * @return an array of split values
   * @throws NullPointerException if value or delimiter is null
   */
  public static String[] splitByDelimiter(String value, String delimiter, int limit) {
    Objects.requireNonNull(value, CoreError.DATA_LOADER_SPLIT_INPUT_VALUE_NULL.buildMessage());
    Objects.requireNonNull(
        delimiter, CoreError.DATA_LOADER_SPLIT_INPUT_DELIMITER_NULL.buildMessage());
    return value.split(delimiter, limit);
  }
}
