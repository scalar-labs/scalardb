package com.scalar.db.dataloader.core.util;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import javax.annotation.Nullable;

/** Utility class for creating and dealing with ScalarDB keys. */
public final class KeyUtils {

  /** Restrict instantiation via private constructor */
  private KeyUtils() {}

  /**
   * Convert a keyValue, in the format of <key>=<value>, to a ScalarDB Key instance for a specific
   * ScalarDB table.
   *
   * @param columnKeyValue A key value in the format of <key>=<value>
   * @param namespace Name of the ScalarDB namespace
   * @param tableName Name of the ScalarDB table
   * @param tableMetadata Metadata for one ScalarDB table
   * @return A new ScalarDB Key instance formatted by data type
   * @throws KeyParsingException if there is an error parsing the key value
   */
  @Nullable
  public static Key parseKeyValue(
      @Nullable ColumnKeyValue columnKeyValue,
      String namespace,
      String tableName,
      TableMetadata tableMetadata)
      throws KeyParsingException {
    if (columnKeyValue == null) {
      return null;
    }
    String columnName = columnKeyValue.getColumnName();
    DataType columnDataType = tableMetadata.getColumnDataType(columnName);
    if (columnDataType == null) {
      throw new KeyParsingException(
          CoreError.DATA_LOADER_INVALID_COLUMN_NON_EXISTENT.buildMessage(
              columnName, tableName, namespace));
    }
    return createKey(columnDataType, columnName, columnKeyValue.getColumnValue());
  }

  /**
   * Create a ScalarDB key based on the provided data type, column name, and value.
   *
   * @param dataType Data type of the specified column
   * @param columnName ScalarDB table column name
   * @param value Value for ScalarDB key
   * @return ScalarDB Key instance
   * @throws KeyParsingException if there is an error while creating a ScalarDB key
   */
  public static Key createKey(DataType dataType, String columnName, String value)
      throws KeyParsingException {
    try {
      Column<?> keyValue = ColumnUtils.createColumnFromValue(dataType, columnName, value);
      return Key.newBuilder().add(keyValue).build();
    } catch (ColumnParsingException e) {
      throw new KeyParsingException(e.getMessage(), e);
    }
  }
}
