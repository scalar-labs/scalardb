package com.scalar.db.dataloader.core.util;

import com.scalar.db.api.TableMetadata;
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

/** Utils for ScalarDB table metadata */
public final class TableMetadataUtils {

  /** Format for the lookup key for a table in a namespace */
  public static final String TABLE_LOOKUP_KEY_FORMAT = "%s.%s";

  /** Private constructor to prevent instantiation */
  private TableMetadataUtils() {}

  /**
   * Check if the field is a ScalarDB transaction metadata column or not
   *
   * @param columnName Table column name
   * @param metadataColumns Fixed list of metadata columns
   * @param columnNames List of all column names in a table
   * @return The field is metadata or not
   */
  public static boolean isMetadataColumn(
      String columnName, Set<String> metadataColumns, Set<String> columnNames) {
    // Skip field if it can be ignored
    if (metadataColumns.contains(columnName)) {
      return true;
    }

    // Skip if the field is a "before_" field
    return columnName.startsWith(Attribute.BEFORE_PREFIX)
        && !columnNames.contains(Attribute.BEFORE_PREFIX + columnName);
  }

  /**
   * Check if the field is a ScalarDB transaction metadata column or not
   *
   * @param columnName ScalarDB table column name5
   * @param tableMetadata Metadata for a single ScalarDB
   * @return is the field a metadata column or not
   */
  public static boolean isMetadataColumn(String columnName, TableMetadata tableMetadata) {
    Set<String> metadataColumns = getMetadataColumns();
    LinkedHashSet<String> columnNames = tableMetadata.getColumnNames();

    // Skip field if it can be ignored
    if (metadataColumns.contains(columnName)) {
      return true;
    }

    // Skip if the field is a "before_" field
    return columnName.startsWith(Attribute.BEFORE_PREFIX)
        && !columnNames.contains(Attribute.BEFORE_PREFIX + columnName);
  }

  /**
   * Return a list of fixed ScalarDB transaction metadata columns
   *
   * @return Set of columns
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
   * Return a map with the data types for all columns in a ScalarDB table
   *
   * @param tableMetadata Metadata for a single ScalarDB table
   * @return data types map
   */
  public static Map<String, DataType> extractColumnDataTypes(TableMetadata tableMetadata) {
    Map<String, DataType> columnDataTypes = new HashMap<>();
    tableMetadata
        .getColumnNames()
        .forEach(
            columnName ->
                columnDataTypes.computeIfAbsent(columnName, tableMetadata::getColumnDataType));
    return columnDataTypes;
  }

  /**
   * Return lookup key for a table in a namespace
   *
   * @param namespace Namespace
   * @param tableName Table name
   * @return Table metadata lookup key
   */
  public static String getTableLookupKey(String namespace, String tableName) {
    return String.format(TABLE_LOOKUP_KEY_FORMAT, namespace, tableName);
  }

  /**
   * Populate the projection columns with ScalarDB transaction metadata columns
   *
   * @param tableMetadata Metadata for a single ScalarDB table
   * @param projections List of projection columns
   * @return List of projection columns with metadata columns
   */
  public static List<String> populateProjectionsWithMetadata(
      TableMetadata tableMetadata, List<String> projections) {
    List<String> projectionMetadata = new ArrayList<>();

    // Add projection columns along with metadata columns
    projections.forEach(
        projection -> {
          projectionMetadata.add(projection);
          if (!isKeyColumn(projection, tableMetadata)) {
            // Add metadata column before the projection if it's not a key column
            projectionMetadata.add(Attribute.BEFORE_PREFIX + projection);
          }
        });

    // Add fixed metadata columns
    projectionMetadata.addAll(getMetadataColumns());

    return projectionMetadata;
  }

  /**
   * Checks if a column is a key column (partition key or clustering key) in the table.
   *
   * @param column The column name to check.
   * @param tableMetadata The metadata of the ScalarDB table.
   * @return True if the column is a key column, false otherwise.
   */
  private static boolean isKeyColumn(String column, TableMetadata tableMetadata) {
    return tableMetadata.getPartitionKeyNames().contains(column)
        || tableMetadata.getClusteringKeyNames().contains(column);
  }
}
