package com.scalar.db.dataloader.core.util;

import com.scalar.db.common.error.CoreError;
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

/** Utility class for dealing and creating ScalarDB Columns */
public final class ColumnUtils {

  /** Restrict instantiation via private constructor */
  private ColumnUtils() {}

  /**
   * Create a ScalarDB column from the given data type, column name, and value. Blob source values
   * need to be base64 encoded.
   *
   * @param dataType Data type of the specified column
   * @param columnName ScalarDB table column name
   * @param value Value for the ScalarDB column
   * @return ScalarDB column
   * @throws ColumnParsingException if an error occurs while creating the column and parsing the
   *     value
   */
  public static Column<?> createColumnFromValue(
      DataType dataType, String columnName, @Nullable String value) throws ColumnParsingException {
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
          CoreError.DATA_LOADER_INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE.buildMessage(columnName), e);
    } catch (IllegalArgumentException e) {
      throw new ColumnParsingException(
          CoreError.DATA_LOADER_INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE.buildMessage(columnName),
          e);
    }
  }
}
