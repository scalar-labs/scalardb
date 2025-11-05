package com.scalar.db.dataloader.cli.util;

import com.scalar.db.dataloader.core.DataLoaderError;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

public class CommandLineInputUtils {

  /**
   * Parses a single key-value pair from a string in the format "key=value".
   *
   * @param keyValue the key-value string to parse
   * @return a {@link java.util.Map.Entry} representing the parsed key-value pair
   * @throws IllegalArgumentException if the input is null, empty, or not in the expected format
   */
  public static Map.Entry<String, String> parseKeyValue(String keyValue) {
    if (StringUtils.isBlank(keyValue)) {
      throw new IllegalArgumentException(
          DataLoaderError.NULL_OR_EMPTY_KEY_VALUE_INPUT.buildMessage());
    }

    String[] parts = splitByDelimiter(keyValue, "=", 2);

    if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
      throw new IllegalArgumentException(
          DataLoaderError.INVALID_KEY_VALUE_INPUT.buildMessage(keyValue));
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
    Objects.requireNonNull(value, DataLoaderError.SPLIT_INPUT_VALUE_NULL.buildMessage());
    Objects.requireNonNull(delimiter, DataLoaderError.SPLIT_INPUT_DELIMITER_NULL.buildMessage());
    return value.split(delimiter, limit);
  }

  /**
   * Validates that a given integer value is positive. If the value is less than 1, it throws a
   * {@link picocli.CommandLine.ParameterException} with the specified error message.
   *
   * @param commandLine the {@link CommandLine} instance used to provide context for the exception
   * @param value the integer value to validate
   * @param error the error that is thrown when the value is invalid
   */
  public static void validatePositiveValue(
      CommandLine commandLine, int value, DataLoaderError error) {
    if (value < 1) {
      throw new CommandLine.ParameterException(commandLine, error.buildMessage());
    }
  }

  /**
   * Checks whether the configuration file specifies the ScalarDB transaction manager as {@code
   * "single-crud-operation"}.
   *
   * <p>This method loads the given properties file, reads the value of the {@code
   * scalar.db.transaction_manager} property, and returns {@code true} if it is set to {@code
   * "single-crud-operation"}; otherwise, it returns {@code false}.
   *
   * <p>If the file cannot be read or an I/O error occurs, the exception is printed to the standard
   * error stream and {@code false} may be returned if the property is missing or null.
   *
   * @param configFilePath the path to the configuration properties file
   * @return {@code true} if the {@code scalar.db.transaction_manager} property is set to {@code
   *     "single-crud-operation"}; {@code false} otherwise
   */
  public static boolean isSingleCrudOperation(String configFilePath) {
    Properties props = new Properties();
    try {
      try (FileInputStream fis = new FileInputStream(configFilePath)) {
        props.load(fis);
      }
    } catch (IOException e) {
      return false; // gracefully handle missing file or read errors
    }
    String trnManager = props.getProperty("scalar.db.transaction_manager");
    return "single-crud-operation".equals(trnManager);
  }

  /**
   * Validates that a deprecated option and its replacement are not both specified. If both options
   * are detected, it throws a {@link picocli.CommandLine.ParameterException} with an appropriate
   * error message.
   *
   * @param commandLine the {@link CommandLine} instance used to provide context for the exception
   * @param deprecatedOption the deprecated option name
   * @param newOption the new option name
   * @param newOptionShort the short form of the new option name
   * @throws CommandLine.ParameterException if both deprecated and new options are specified
   */
  public static void validateDeprecatedOptionPair(
      CommandLine commandLine, String deprecatedOption, String newOption, String newOptionShort) {
    boolean hasDeprecated = commandLine.getParseResult().hasMatchedOption(deprecatedOption);
    boolean hasNew =
        commandLine.getParseResult().hasMatchedOption(newOption)
            || commandLine.getParseResult().hasMatchedOption(newOptionShort);

    if (hasDeprecated && hasNew) {
      throw new CommandLine.ParameterException(
          commandLine,
          DataLoaderError.DEPRECATED_AND_NEW_OPTION_BOTH_SPECIFIED.buildMessage(
              deprecatedOption, newOption, newOption));
    }
  }
}
