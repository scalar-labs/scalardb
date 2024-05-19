package com.scalar.db.dataloader.core.util;

import static com.scalar.db.dataloader.core.constant.ErrorMessages.ERROR_BASE64_ENCODING;
import static com.scalar.db.dataloader.core.constant.ErrorMessages.ERROR_NUMBER_FORMAT_EXCEPTION;

import com.scalar.db.dataloader.core.exception.Base64Exception;
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

/** Utility class for dealing and creating ScalarDB Columns */
public class ColumnUtils {
  private ColumnUtils() {
    // restrict instantiation
  }

  /**
   * Create a ScalarDB column from the given data type, column name, and value. Blob source values
   * need to be base64 encoded.
   *
   * @param dataType Data type of the specified column
   * @param columnName ScalarDB table column name
   * @param value Value for the ScalarDB column
   * @return ScalarDB column
   * @throws Base64Exception if an error occurs while base64 decoding
   */
  public static Column<?> createColumnFromValue(DataType dataType, String columnName, String value)
      throws Base64Exception {
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
      throw new NumberFormatException(String.format(ERROR_NUMBER_FORMAT_EXCEPTION, columnName));
    } catch (IllegalArgumentException e) {
      throw new Base64Exception(String.format(ERROR_BASE64_ENCODING, columnName));
    }
  }
}
