package com.scalar.db.dataloader.core.util;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;

/** Utility class for creating and dealing with ScalarDB keys. */
public class KeyUtils {

  private KeyUtils() {
    // restrict instantiation
  }

  /**
   * Convert a keyValue, in the format of <key>=<value>, to a ScalarDB Key instance.
   *
   * @param keyValue A key value in the format of <key>=<value>
   * @param tableMetadata Metadata for one ScalarDB table
   * @return A new ScalarDB Key instance formatted by data type
   * @throws KeyParsingException if there is an error parsing the key value
   */
  public static Key parseKeyValue(ColumnKeyValue keyValue, TableMetadata tableMetadata)
      throws KeyParsingException {
    if (keyValue == null) {
      return null;
    }
    String columnName = keyValue.getColumnName();
    DataType columnDataType = tableMetadata.getColumnDataType(columnName);
    if (columnDataType == null) {
      throw new KeyParsingException(
          "Invalid key: Column " + columnName + " does not exist in the table.");
    }
    try {
      return createKey(columnDataType, columnName, keyValue.getColumnValue());
    } catch (Base64Exception e) {
      throw new KeyParsingException("Invalid key value: " + e.getMessage(), e);
    }
  }

  /**
   * Create a ScalarDB key based on the provided data type, column name, and value.
   *
   * @param dataType Data type of the specified column
   * @param columnName ScalarDB table column name
   * @param value Value for ScalarDB key
   * @return ScalarDB Key instance
   * @throws Base64Exception if there is an error creating the key value
   */
  public static Key createKey(DataType dataType, String columnName, String value)
      throws Base64Exception {
    Column<?> keyValue = ColumnUtils.createColumnFromValue(dataType, columnName, value);
    return Key.newBuilder().add(keyValue).build();
  }
}
