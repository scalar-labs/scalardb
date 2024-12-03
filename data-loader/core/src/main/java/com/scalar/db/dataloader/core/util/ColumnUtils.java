package com.scalar.db.dataloader.core.util;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.util.Base64;
import javax.annotation.Nullable;

/**
 * Utility class for creating and managing ScalarDB columns.
 * <p>
 * This class provides methods for creating ScalarDB columns based on the given data type, column
 * information, and value. It includes handling for various data types and special cases like
 * base64 encoding for BLOB data.
 * </p>
 */
public final class ColumnUtils {

  /** Restrict instantiation via private constructor */
  private ColumnUtils() {}

  /**
   * Creates a ScalarDB column from the given data type, column information, and value.
   * <p>
   * Blob source values need to be base64 encoded before passing them as a value. If the value
   * is {@code null}, the corresponding column is created as a {@code null} column.
   * </p>
   *
   * @param dataType the data type of the specified column
   * @param columnInfo the ScalarDB table column information
   * @param value the value for the ScalarDB column (may be {@code null})
   * @return the ScalarDB column created from the specified data
   * @throws ColumnParsingException if an error occurs while creating the column or parsing the value
   */
  public static Column<?> createColumnFromValue(
          DataType dataType, ColumnInfo columnInfo, @Nullable String value)
          throws ColumnParsingException {
    String columnName = columnInfo.getColumnName();
    try {
      switch (dataType) {
        case BOOLEAN:
          return value != null
                  ? BooleanColumn.of(columnName, Boolean.parseBoolean(value))
                  : BooleanColumn.ofNull(columnName);
        case INT:
          return value != null
                  ? IntColumn.of(columnName, Integer.parseInt(value))
                  : IntColumn.ofNull(columnName);
        case BIGINT:
          return value != null
                  ? BigIntColumn.of(columnName, Long.parseLong(value))
                  : BigIntColumn.ofNull(columnName);
        case FLOAT:
          return value != null
                  ? FloatColumn.of(columnName, Float.parseFloat(value))
                  : FloatColumn.ofNull(columnName);
        case DOUBLE:
          return value != null
                  ? DoubleColumn.of(columnName, Double.parseDouble(value))
                  : DoubleColumn.ofNull(columnName);
        case TEXT:
          return value != null ? TextColumn.of(columnName, value) : TextColumn.ofNull(columnName);
        case BLOB:
          // Source blob values need to be base64 encoded
          return value != null
                  ? BlobColumn.of(columnName, Base64.getDecoder().decode(value))
                  : BlobColumn.ofNull(columnName);
        default:
          throw new AssertionError();
      }
    } catch (NumberFormatException e) {
      throw new ColumnParsingException(
              CoreError.DATA_LOADER_INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE.buildMessage(
                      columnName, columnInfo.getTableName(), columnInfo.getNamespace()),
              e);
    } catch (IllegalArgumentException e) {
      throw new ColumnParsingException(
              CoreError.DATA_LOADER_INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE.buildMessage(
                      columnName, columnInfo.getTableName(), columnInfo.getNamespace()),
              e);
    }
  }
}
