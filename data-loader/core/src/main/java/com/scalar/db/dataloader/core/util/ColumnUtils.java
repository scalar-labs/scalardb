package com.scalar.db.dataloader.core.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class for creating and managing ScalarDB columns.
 *
 * <p>This class provides utility methods for:
 *
 * <ul>
 *   <li>Creating ScalarDB columns from various data types and values
 *   <li>Converting between ScalarDB Result objects and column data
 *   <li>Handling special data formats like base64 encoding for BLOB data
 *   <li>Managing transaction-related metadata columns
 * </ul>
 *
 * <p>The class supports all ScalarDB data types including:
 *
 * <ul>
 *   <li>Basic types: BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, TEXT
 *   <li>Binary data: BLOB (requires base64 encoding)
 *   <li>Temporal types: DATE, TIME, TIMESTAMP, TIMESTAMPTZ
 * </ul>
 */
public final class ColumnUtils {

  /** Restrict instantiation via private constructor */
  private ColumnUtils() {}

  /**
   * Creates a ScalarDB column from the given data type, column information, and value.
   *
   * <p>This method handles the creation of columns for all supported ScalarDB data types. For BLOB
   * type columns, the input value must be base64 encoded before being passed to this method.
   *
   * <p>If the provided value is {@code null}, a null column of the appropriate type is created.
   *
   * @param dataType the data type of the specified column (e.g., BOOLEAN, INT, TEXT, etc.)
   * @param columnInfo the ScalarDB table column information containing column name and metadata
   * @param value the string representation of the value for the ScalarDB column (maybe {@code
   *     null})
   * @return the ScalarDB column created from the specified data
   * @throws ColumnParsingException if an error occurs while creating the column, such as:
   *     <ul>
   *       <li>Invalid number format for numeric types
   *       <li>Invalid base64 encoding for BLOB type
   *       <li>Invalid date/time format for temporal types
   *     </ul>
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
        case DATE:
          return value != null
              ? DateColumn.of(columnName, LocalDate.parse(value))
              : DateColumn.ofNull(columnName);
        case TIME:
          return value != null
              ? TimeColumn.of(columnName, LocalTime.parse(value))
              : TimeColumn.ofNull(columnName);
        case TIMESTAMP:
          return value != null
              ? TimestampColumn.of(columnName, LocalDateTime.parse(value))
              : TimestampColumn.ofNull(columnName);
        case TIMESTAMPTZ:
          return value != null
              ? TimestampTZColumn.of(columnName, Instant.parse(value))
              : TimestampTZColumn.ofNull(columnName);
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
   * Retrieves columns from a ScalarDB Result object, comparing with source data and handling
   * metadata.
   *
   * <p>This method processes the result data while:
   *
   * <ul>
   *   <li>Excluding transaction metadata columns
   *   <li>Excluding partition and clustering key columns
   *   <li>Handling null values based on the ignoreNullValues parameter
   *   <li>Merging data from both ScalarDB Result and source record
   * </ul>
   *
   * @param scalarDBResult the ScalarDB Result object containing the current data
   * @param sourceRecord the source data in JSON format to compare against
   * @param ignoreNullValues if true, null values will be excluded from the result
   * @param tableMetadata metadata about the table structure and column types
   * @return a List of Column objects representing the processed data
   * @throws Base64Exception if there's an error processing base64 encoded BLOB data
   * @throws ColumnParsingException if there's an error parsing column values
   */
  public static List<Column<?>> getColumnsFromResult(
      Result scalarDBResult,
      JsonNode sourceRecord,
      boolean ignoreNullValues,
      TableMetadata tableMetadata)
      throws Base64Exception, ColumnParsingException {

    List<Column<?>> columns = new ArrayList<>();
    Set<String> columnsToIgnore =
        getColumnsToIgnore(
            tableMetadata.getPartitionKeyNames(), tableMetadata.getClusteringKeyNames());
    for (String columnName : tableMetadata.getColumnNames()) {
      if (ConsensusCommitUtils.isTransactionMetaColumn(columnName, tableMetadata)
          || columnsToIgnore.contains(columnName)) {
        continue;
      }

      Column<?> column =
          getColumn(
              scalarDBResult,
              sourceRecord,
              columnName,
              ignoreNullValues,
              tableMetadata.getColumnDataTypes());

      if (column != null) {
        columns.add(column);
      }
    }

    return columns;
  }

  /**
   * Creates a set of column names that should be ignored during processing.
   *
   * <p>This method combines:
   *
   * <ul>
   *   <li>Transaction metadata columns
   *   <li>Partition key columns
   *   <li>Clustering key columns
   * </ul>
   *
   * @param partitionKeyNames set of column names that are partition keys
   * @param clusteringKeyNames set of column names that are clustering keys
   * @return a Set of column names that should be ignored during processing
   */
  private static Set<String> getColumnsToIgnore(
      Set<String> partitionKeyNames, Set<String> clusteringKeyNames) {
    Set<String> columnsToIgnore =
        new HashSet<>(ConsensusCommitUtils.getTransactionMetaColumns().keySet());
    columnsToIgnore.addAll(partitionKeyNames);
    columnsToIgnore.addAll(clusteringKeyNames);
    return columnsToIgnore;
  }

  /**
   * Retrieves a column value by comparing ScalarDB Result data with source record data.
   *
   * <p>This method determines which data source to use for the column value:
   *
   * <ul>
   *   <li>If the column exists in ScalarDB Result but not in source record, uses Result data
   *   <li>Otherwise, uses the source record data
   * </ul>
   *
   * @param scalarDBResult the ScalarDB Result object containing current data
   * @param sourceRecord the source data in JSON format
   * @param columnName the name of the column to retrieve
   * @param ignoreNullValues whether to ignore null values in the result
   * @param dataTypesByColumns mapping of column names to their data types
   * @return the Column object containing the value, or null if ignored
   * @throws ColumnParsingException if there's an error parsing the column value
   */
  private static Column<?> getColumn(
      Result scalarDBResult,
      JsonNode sourceRecord,
      String columnName,
      boolean ignoreNullValues,
      Map<String, DataType> dataTypesByColumns)
      throws ColumnParsingException {
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
   * @throws ColumnParsingException if an error occurs while parsing the column
   */
  private static Column<?> getColumnFromSourceRecord(
      JsonNode sourceRecord,
      String columnName,
      boolean ignoreNullValues,
      Map<String, DataType> dataTypesByColumns)
      throws ColumnParsingException {
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
