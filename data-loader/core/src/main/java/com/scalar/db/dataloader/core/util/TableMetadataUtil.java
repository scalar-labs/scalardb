package com.scalar.db.dataloader.core.util;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.Constants;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.Attribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class for handling ScalarDB table metadata operations. */
public class TableMetadataUtil {

  /**
   * Determines whether a given column is a metadata column based on predefined criteria.
   *
   * @param columnName The name of the table column to check.
   * @param metadataColumns A set of predefined metadata columns.
   * @param columnNames A set of all column names in the table.
   * @return {@code true} if the column is a metadata column; {@code false} otherwise.
   */
  public static boolean isMetadataColumn(
      String columnName, Set<String> metadataColumns, Set<String> columnNames) {
    if (metadataColumns.contains(columnName)) {
      return true;
    }
    return columnName.startsWith(Attribute.BEFORE_PREFIX)
        && !columnNames.contains(Attribute.BEFORE_PREFIX + columnName);
  }

  /**
   * Determines whether a given column is a metadata column using table metadata.
   *
   * @param columnName The name of the ScalarDB table column to check.
   * @param tableMetadata The metadata of the table.
   * @return {@code true} if the column is a metadata column; {@code false} otherwise.
   */
  public static boolean isMetadataColumn(String columnName, TableMetadata tableMetadata) {
    Set<String> metadataColumns = getMetadataColumns();
    LinkedHashSet<String> columnNames = tableMetadata.getColumnNames();
    return isMetadataColumn(columnName, metadataColumns, columnNames);
  }

  /**
   * Retrieves a set of fixed metadata column names used in ScalarDB.
   *
   * @return A set of predefined metadata column names.
   */
  public static Set<String> getMetadataColumns() {
    return Stream.of(
            Attribute.ID,
            Attribute.STATE,
            Attribute.VERSION,
            Attribute.PREPARED_AT,
            Attribute.COMMITTED_AT,
            Attribute.BEFORE_ID,
            Attribute.BEFORE_STATE,
            Attribute.BEFORE_VERSION,
            Attribute.BEFORE_PREPARED_AT,
            Attribute.BEFORE_COMMITTED_AT)
        .collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Extracts a mapping of column names to their data types from the table metadata.
   *
   * @param tableMetadata The metadata of the ScalarDB table.
   * @return A map where keys are column names and values are their corresponding {@link DataType}.
   */
  public static Map<String, DataType> extractColumnDataTypes(TableMetadata tableMetadata) {
    Map<String, DataType> definitions = new HashMap<>();
    for (String columnName : tableMetadata.getColumnNames()) {
      definitions.put(columnName, tableMetadata.getColumnDataType(columnName));
    }
    return definitions;
  }

  /**
   * Generates a unique lookup key for a table within a namespace.
   *
   * @param namespace The namespace of the table.
   * @param tableName The name of the table.
   * @return A formatted string representing the table lookup key.
   */
  public static String getTableLookupKey(String namespace, String tableName) {
    return String.format(Constants.TABLE_LOOKUP_KEY_FORMAT, namespace, tableName);
  }

  /**
   * Generates a unique lookup key for a table using control file table data.
   *
   * @param controlFileTable The control file table object containing namespace and table name.
   * @return A formatted string representing the table lookup key.
   */
  public static String getTableLookupKey(ControlFileTable controlFileTable) {
    return String.format(
        Constants.TABLE_LOOKUP_KEY_FORMAT,
        controlFileTable.getNamespace(),
        controlFileTable.getTableName());
  }

  /**
   * Adds metadata columns to a list of projection columns for a ScalarDB table.
   *
   * @param tableMetadata The metadata of the ScalarDB table.
   * @param projections A list of projection column names.
   * @return A new list containing projection columns along with metadata columns.
   */
  public static List<String> populateProjectionsWithMetadata(
      TableMetadata tableMetadata, List<String> projections) {
    List<String> projectionMetadata = new ArrayList<>();
    projections.forEach(
        projection -> {
          projectionMetadata.add(projection);
          if (!isKeyColumn(projection, tableMetadata)) {
            projectionMetadata.add(Attribute.BEFORE_PREFIX + projection);
          }
        });
    projectionMetadata.addAll(getMetadataColumns());
    return projectionMetadata;
  }

  /**
   * Checks whether a column is a key column (partition key or clustering key) in the table.
   *
   * @param column The name of the column to check.
   * @param tableMetadata The metadata of the ScalarDB table.
   * @return {@code true} if the column is a key column; {@code false} otherwise.
   */
  private static boolean isKeyColumn(String column, TableMetadata tableMetadata) {
    return tableMetadata.getPartitionKeyNames().contains(column)
        || tableMetadata.getClusteringKeyNames().contains(column);
  }
}
