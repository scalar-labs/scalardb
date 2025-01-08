package com.scalar.db.dataloader.core.util;

import static com.scalar.db.dataloader.core.util.TableMetadataUtil.isMetadataColumn;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.api.Result;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.exception.Base64Exception;
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
import java.util.*;
import javax.annotation.Nullable;

/**
 * Utility class for creating and managing ScalarDB columns.
 *
 * <p>This class provides methods for creating ScalarDB columns based on the given data type, column
 * information, and value. It includes handling for various data types and special cases like base64
 * encoding for BLOB data.
 */
public final class ColumnUtils {

  /** Restrict instantiation via private constructor */
  private ColumnUtils() {}

  /**
   * Creates a ScalarDB column from the given data type, column information, and value.
   *
   * <p>Blob source values need to be base64 encoded before passing them as a value. If the value is
   * {@code null}, the corresponding column is created as a {@code null} column.
   *
   * @param dataType the data type of the specified column
   * @param columnInfo the ScalarDB table column information
   * @param value the value for the ScalarDB column (may be {@code null})
   * @return the ScalarDB column created from the specified data
   * @throws ColumnParsingException if an error occurs while creating the column or parsing the
   *     value
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

  /**
   * Get columns from result data
   *
   * @param scalarDBResult result record
   * @param sourceRecord source data
   * @param ignoreNullValues ignore null values or not
   * @param partitionKeyNames partition key names
   * @param clusteringKeyNames clustering key names
   * @param columnNames column names
   * @param dataTypesByColumns data types of columns
   * @return list of columns
   * @throws Base64Exception if an error occurs while base64 decoding
   */
  public static List<Column<?>> getColumnsFromResult(
      Result scalarDBResult,
      JsonNode sourceRecord,
      boolean ignoreNullValues,
      Set<String> partitionKeyNames,
      Set<String> clusteringKeyNames,
      Set<String> columnNames,
      Map<String, DataType> dataTypesByColumns)
      throws Base64Exception, ColumnParsingException {

    List<Column<?>> columns = new ArrayList<>();
    Set<String> columnsToIgnore = getColumnsToIgnore(partitionKeyNames, clusteringKeyNames);

    for (String columnName : columnNames) {
      if (isMetadataColumn(columnName, columnsToIgnore, columnNames)) {
        continue;
      }

      Column<?> column =
          getColumn(scalarDBResult, sourceRecord, columnName, ignoreNullValues, dataTypesByColumns);

      if (column != null) {
        columns.add(column);
      }
    }

    return columns;
  }

  /**
   * Create a set of columns to ignore
   *
   * @param partitionKeyNames a set of partition key names
   * @param clusteringKeyNames a set of clustering key names
   * @return a set of columns to ignore
   */
  private static Set<String> getColumnsToIgnore(
      Set<String> partitionKeyNames, Set<String> clusteringKeyNames) {
    Set<String> columnsToIgnore = new HashSet<>(TableMetadataUtil.getMetadataColumns());
    columnsToIgnore.addAll(partitionKeyNames);
    columnsToIgnore.addAll(clusteringKeyNames);
    return columnsToIgnore;
  }

  /**
   * Checks if a column is a metadata column
   *
   * @param columnName column name
   * @param columnsToIgnore set of columns to ignore
   * @param columnNames set of column names
   * @return if column is a metadata column or not
   */
  private static boolean isMetadataColumn(
      String columnName, Set<String> columnsToIgnore, Set<String> columnNames) {
    return TableMetadataUtil.isMetadataColumn(columnName, columnsToIgnore, columnNames);
  }

  /**
   * Get columns from result data
   *
   * @param scalarDBResult result record
   * @param sourceRecord source data
   * @param columnName column name
   * @param ignoreNullValues ignore null values or not
   * @param dataTypesByColumns data types of columns
   * @return column data
   * @throws Base64Exception if an error occurs while base64 decoding
   */
  private static Column<?> getColumn(
      Result scalarDBResult,
      JsonNode sourceRecord,
      String columnName,
      boolean ignoreNullValues,
      Map<String, DataType> dataTypesByColumns)
      throws Base64Exception, ColumnParsingException {
    if (scalarDBResult != null && !sourceRecord.has(columnName)) {
      return getColumnFromResult(scalarDBResult, columnName);
    } else {
      return getColumnFromSourceRecord(
          sourceRecord, columnName, ignoreNullValues, dataTypesByColumns);
    }
  }

  /**
   * Get column from result
   *
   * @param scalarDBResult result record
   * @param columnName column name
   * @return column data
   */
  private static Column<?> getColumnFromResult(Result scalarDBResult, String columnName) {
    Map<String, Column<?>> columnValues = scalarDBResult.getColumns();
    return columnValues.get(columnName);
  }

  /**
   * Get column from result
   *
   * @param sourceRecord source data
   * @param columnName column name
   * @param ignoreNullValues ignore null values or not
   * @param dataTypesByColumns data types of columns
   * @return column data
   * @throws Base64Exception if an error occurs while base64 decoding
   */
  private static Column<?> getColumnFromSourceRecord(
      JsonNode sourceRecord,
      String columnName,
      boolean ignoreNullValues,
      Map<String, DataType> dataTypesByColumns)
      throws Base64Exception, ColumnParsingException {
    DataType dataType = dataTypesByColumns.get(columnName);
    String columnValue =
        sourceRecord.has(columnName) && !sourceRecord.get(columnName).isNull()
            ? sourceRecord.get(columnName).asText()
            : null;
    if (!ignoreNullValues || columnValue != null) {
      ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
      return createColumnFromValue(dataType, columnInfo, columnValue);
    }
    return null;
  }
}
