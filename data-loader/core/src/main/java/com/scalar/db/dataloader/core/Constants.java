package com.scalar.db.dataloader.core;

/** The constants that are used in the com.scalar.dataloader.core package */
public class Constants {
  /**
   * Format string used for table lookup keys. It expects two string arguments: the namespace and
   * the table name, respectively.
   *
   * <p>Example: {@code String.format(TABLE_LOOKUP_KEY_FORMAT, "ns", "table")} will result in
   * "ns.table".
   */
  public static final String TABLE_LOOKUP_KEY_FORMAT = "%s.%s";
  /**
   * Status message used to indicate that a transaction was aborted as part of a batch transaction
   * failure.
   */
  public static final String ABORT_TRANSACTION_STATUS =
      "Transaction aborted as part of batch transaction aborted";
  /**
   * Special null value representation used for TEXT data type columns in CSV files.
   *
   * <p>This value is used to distinguish between an empty string and a null value in CSV exports
   * and imports. When exporting, null TEXT values are converted to this string. When importing,
   * this string is converted back to null for TEXT columns.
   */
  public static final String CSV_TEXT_NULL_VALUE = "\\N";
}
