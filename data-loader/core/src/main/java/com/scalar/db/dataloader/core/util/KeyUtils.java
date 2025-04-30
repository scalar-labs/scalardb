package com.scalar.db.dataloader.core.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class for creating and managing ScalarDB keys.
 *
 * <p>This class provides utility methods for:
 *
 * <ul>
 *   <li>Creating partition and clustering keys from source records
 *   <li>Parsing key-value pairs into ScalarDB Key instances
 *   <li>Creating composite keys from multiple columns
 * </ul>
 *
 * <p>The class handles proper type conversion and validation of keys according to the table
 * metadata and column data types. It also provides comprehensive error handling for various
 * key-related operations.
 */
public final class KeyUtils {

  /** Restrict instantiation via private constructor */
  private KeyUtils() {}

  /**
   * Creates an {@link Optional} clustering key from the given source record.
   *
   * <p>This method constructs a clustering key by extracting values from the source record for each
   * clustering key column. If any required clustering key column is missing from the source record
   * or if there's an error in data conversion, an empty Optional is returned.
   *
   * @param clusteringKeyNames A set of column names that make up the clustering key
   * @param dataTypeByColumnName A map defining the data type for each column name
   * @param sourceRecord The source record containing the column values
   * @return An {@link Optional} containing the clustering key if all required columns exist and are
   *     valid, otherwise {@link Optional#empty()}
   */
  public static Optional<Key> createClusteringKeyFromSource(
      Set<String> clusteringKeyNames,
      Map<String, DataType> dataTypeByColumnName,
      ObjectNode sourceRecord) {
    return clusteringKeyNames.isEmpty()
        ? Optional.empty()
        : createKeyFromSource(clusteringKeyNames, dataTypeByColumnName, sourceRecord);
  }

  /**
   * Creates an {@link Optional} partition key from the given source record.
   *
   * <p>This method constructs a partition key by extracting values from the source record for each
   * partition key column. If any required partition key column is missing from the source record or
   * if there's an error in data conversion, an empty Optional is returned.
   *
   * @param partitionKeyNames A set of column names that make up the partition key
   * @param dataTypeByColumnName A map defining the data type for each column name
   * @param sourceRecord The source record containing the column values
   * @return An {@link Optional} containing the partition key if all required columns exist and are
   *     valid, otherwise {@link Optional#empty()}
   */
  public static Optional<Key> createPartitionKeyFromSource(
      Set<String> partitionKeyNames,
      Map<String, DataType> dataTypeByColumnName,
      ObjectNode sourceRecord) {
    return createKeyFromSource(partitionKeyNames, dataTypeByColumnName, sourceRecord);
  }

  /**
   * Converts a key-value pair into a ScalarDB Key instance for a specific ScalarDB table.
   *
   * <p>This method performs the following steps:
   *
   * <ol>
   *   <li>Validates that the column exists in the table metadata
   *   <li>Determines the correct data type for the column
   *   <li>Converts the value to the appropriate type
   *   <li>Creates and returns a new ScalarDB Key instance
   * </ol>
   *
   * @param columnKeyValue A key-value pair containing the column name and value
   * @param namespace The name of the ScalarDB namespace
   * @param tableName The name of the ScalarDB table
   * @param tableMetadata Metadata for the ScalarDB table
   * @return A new ScalarDB Key instance formatted according to the data type, or null if
   *     columnKeyValue is null
   * @throws KeyParsingException If the column doesn't exist in the table or if there's an error
   *     parsing the value
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
   *
   * <p>This method handles the conversion of string values to their appropriate ScalarDB data types
   * and constructs a single-column key. The method ensures type safety and proper formatting of the
   * key value according to the specified data type.
   *
   * @param dataType The data type of the specified column
   * @param columnInfo The ScalarDB table column information
   * @param value The string value to be converted and used as the key
   * @return A ScalarDB Key instance containing the converted value
   * @throws KeyParsingException If there's an error converting the value to the specified data type
   *     or creating the key
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

  /**
   * Creates a new composite ScalarDB key from multiple columns.
   *
   * <p>This method creates a composite key by combining multiple columns, each with its own data
   * type and value. The method requires that all input lists (dataTypes, columnNames, and values)
   * have the same length. If the lists are not of equal length, an empty Optional is returned.
   *
   * <p>The method performs the following for each column:
   *
   * <ol>
   *   <li>Creates a ColumnInfo instance
   *   <li>Converts the string value to the appropriate data type
   *   <li>Adds the converted value to the composite key
   * </ol>
   *
   * @param dataTypes List of data types for each column in the composite key
   * @param columnNames List of column names corresponding to each data type
   * @param values List of string values to be converted and used in the key
   * @return An Optional containing the composite ScalarDB Key if successful, or empty if the input
   *     lists have different lengths
   * @throws Base64Exception If there's an error processing Base64-encoded values
   * @throws ColumnParsingException If there's an error converting any value to its specified data
   *     type
   */
  public static Optional<Key> createCompositeKey(
      List<DataType> dataTypes, List<String> columnNames, List<String> values)
      throws Base64Exception, ColumnParsingException {
    if (!CollectionUtil.areSameLength(dataTypes, columnNames, values)) {
      return Optional.empty();
    }
    Key.Builder builder = Key.newBuilder();
    for (int i = 0; i < dataTypes.size(); i++) {
      ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnNames.get(i)).build();
      Column<?> keyValue =
          ColumnUtils.createColumnFromValue(dataTypes.get(i), columnInfo, values.get(i));
      builder.add(keyValue);
    }
    return Optional.of(builder.build());
  }

  private static Optional<Key> createKeyFromSource(
      Set<String> keyNames, Map<String, DataType> columnDataTypes, JsonNode sourceRecord) {
    List<DataType> dataTypes = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (String keyName : keyNames) {
      if (!columnDataTypes.containsKey(keyName) || !sourceRecord.has(keyName)) {
        return Optional.empty();
      }
      dataTypes.add(columnDataTypes.get(keyName));
      columnNames.add(keyName);
      values.add(sourceRecord.get(keyName).asText());
    }

    try {
      return createCompositeKey(dataTypes, columnNames, values);
    } catch (Base64Exception | ColumnParsingException e) {
      return Optional.empty();
    }
  }

  /**
   * Convert a keyValue, in the format of <key>=<value>, to a ScalarDB Key instance.
   *
   * @param keyValues A list of key values in the format of <key>=<value>
   * @param tableMetadata Metadata for one ScalarDB table
   * @return A new ScalarDB Key instance formatted by data type
   * @throws ColumnParsingException if there is an error parsing the column
   */
  public static Key parseMultipleKeyValues(
      List<ColumnKeyValue> keyValues, TableMetadata tableMetadata) throws ColumnParsingException {
    Key.Builder builder = Key.newBuilder();
    for (ColumnKeyValue keyValue : keyValues) {
      String columnName = keyValue.getColumnName();
      String value = keyValue.getColumnValue();
      DataType columnDataType = tableMetadata.getColumnDataType(columnName);
      ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
      Column<?> keyValueCol = ColumnUtils.createColumnFromValue(columnDataType, columnInfo, value);
      builder.add(keyValueCol);
    }
    return builder.build();
  }
}
