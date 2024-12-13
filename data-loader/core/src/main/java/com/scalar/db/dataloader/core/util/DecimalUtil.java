package com.scalar.db.dataloader.core.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/** Utils for decimal handling */
public class DecimalUtil {

  /**
   * Convert a Double to a non-scientific formatted string
   *
   * @param doubleValue Double value
   * @return formatted double as a string
   */
  public static String convertToNonScientific(Double doubleValue) {
    return createFormatter().format(doubleValue);
  }

  /**
   * Convert a Float to a non-scientific formatted string
   *
   * @param floatValue Float value
   * @return formatted float as a string
   */
  public static String convertToNonScientific(Float floatValue) {
    return createFormatter().format(floatValue);
  }

  /**
   * Create a Decimal formatter
   *
   * @return decimal formatter instance
   */
  private static DecimalFormat createFormatter() {
    DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
    df.setMaximumFractionDigits(340); // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS
    return df;
  }
}
