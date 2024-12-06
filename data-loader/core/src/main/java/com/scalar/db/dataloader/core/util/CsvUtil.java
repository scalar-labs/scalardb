package com.scalar.db.dataloader.core.util;

/** Utils for csv data manipulation */
public class CsvUtil {

  /**
   * Remove the last character in the string builder if it's a delimiter
   *
   * @param stringBuilder String builder instance
   * @param delimiter Delimiter character used in the CSV content
   */
  public static void removeTrailingDelimiter(StringBuilder stringBuilder, String delimiter) {
    if (stringBuilder.substring(stringBuilder.length() - 1).equals(delimiter)) {
      stringBuilder.setLength(stringBuilder.length() - 1);
    }
  }
}
