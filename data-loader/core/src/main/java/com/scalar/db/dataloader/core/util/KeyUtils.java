package com.scalar.db.dataloader.core.util;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import javax.annotation.Nullable;

/**
 * Utility class for creating and managing ScalarDB keys.
 * <p>
 * This class provides methods to parse key-value pairs and create ScalarDB key instances.
 * It also includes utility methods for handling data types, columns, and potential parsing exceptions.
 * </p>
 */
public final class KeyUtils {

  /** Restrict instantiation via private constructor */
  private KeyUtils() {}

  /**
   * Converts a key-value pair, in the format of <key>=<value>, into a ScalarDB Key instance for a
   * specific ScalarDB table.
   * <p>
   * This method uses the provided table metadata to determine the data type for the key and
   * creates a corresponding ScalarDB Key. If the key does not match any column in the table
   * metadata, a {@link KeyParsingException} is thrown.
   * </p>
   *
   * @param columnKeyValue a key-value pair in the format of <key>=<value>
   * @param namespace the name of the ScalarDB namespace
   * @param tableName the name of the ScalarDB table
   * @param tableMetadata metadata for the ScalarDB table
   * @return a new ScalarDB Key instance formatted according to the data type
   * @throws KeyParsingException if there is an error parsing the key value or if the column does not exist
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
    ColumnInfo columnInfo =
            ColumnInfo.builder()
                    .namespace(namespace)
                    .tableName(tableName)
                    .columnName(columnName)
                    .build();
    return createKey(columnDataType, columnInfo, columnKeyValue.getColumnValue());
  }

  /**
   * Creates a ScalarDB key based on the provided data type, column information, and value.
   * <p>
   * This method creates a ScalarDB Key instance by converting the column value to the appropriate
   * data type and constructing the key using that value.
   * </p>
   *
   * @param dataType the data type of the specified column
   * @param columnInfo the ScalarDB table column information
   * @param value the value for the ScalarDB key
   * @return a ScalarDB Key instance
   * @throws KeyParsingException if there is an error while creating the ScalarDB key
   */
  public static Key createKey(DataType dataType, ColumnInfo columnInfo, String value)
          throws KeyParsingException {
    try {
      Column<?> keyValue = ColumnUtils.createColumnFromValue(dataType, columnInfo, value);
      return Key.newBuilder().add(keyValue).build();
    } catch (ColumnParsingException e) {
      throw new KeyParsingException(e.getMessage(), e);
    }
  }
}
