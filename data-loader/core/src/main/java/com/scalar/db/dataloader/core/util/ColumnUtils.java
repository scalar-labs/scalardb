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
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.util.ArrayList;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * @param value the value for the ScalarDB column (maybe {@code null})
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
   * Get columns from result data
   *
   * @param scalarDBResult result record
   * @param sourceRecord source data
   * @param ignoreNullValues ignore null values or not
   * @return list of columns
   * @throws Base64Exception if an error occurs while base64 decoding
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
   * Create a set of columns to ignore
   *
   * @param partitionKeyNames a set of partition key names
   * @param clusteringKeyNames a set of clustering key names
   * @return a set of columns to ignore
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
   * Get columns from result data
   *
   * @param scalarDBResult result record
   * @param sourceRecord source data
   * @param columnName column name
   * @param ignoreNullValues ignore null values or not
   * @param dataTypesByColumns data types of columns
   * @return column data
   * @throws ColumnParsingException if an error occurs while base64 parsing the column
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
