package com.scalar.db.dataloader.core.dataexport.validation;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A validator for ensuring that export options are consistent with the ScalarDB table metadata and
 * follow the defined constraints.
 */
@SuppressWarnings("SameNameButDifferent")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExportOptionsValidator {

  /**
   * Validates the export request.
   *
   * @param exportOptions The export options provided by the user.
   * @param tableMetadata The metadata of the ScalarDB table to validate against.
   * @throws ExportOptionsValidationException If the export options are invalid.
   */
  public static void validate(ExportOptions exportOptions, TableMetadata tableMetadata)
      throws ExportOptionsValidationException {
    LinkedHashSet<String> partitionKeyNames = tableMetadata.getPartitionKeyNames();
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    ScanRange scanRange = exportOptions.getScanRange();

    validatePartitionKey(partitionKeyNames, exportOptions.getScanPartitionKey());
    validateProjectionColumns(tableMetadata.getColumnNames(), exportOptions.getProjectionColumns());
    validateSortOrders(clusteringKeyNames, exportOptions.getSortOrders());

    if (scanRange.getScanStartKey() != null) {
      validateClusteringKey(clusteringKeyNames, scanRange.getScanStartKey());
    }
    if (scanRange.getScanEndKey() != null) {
      validateClusteringKey(clusteringKeyNames, scanRange.getScanEndKey());
    }
  }

  /*
   * Check if the provided partition key is available in the ScalarDB table
   * @param partitionKeyNames List of partition key names available in a
   * @param key To be validated ScalarDB key
   * @throws ExportOptionsValidationException if the key could not be found or is not a partition
   */
  private static void validatePartitionKey(LinkedHashSet<String> partitionKeyNames, Key key)
      throws ExportOptionsValidationException {
    if (partitionKeyNames == null || key == null) {
      return;
    }

    // Make sure that all partition key columns are provided
    if (partitionKeyNames.size() != key.getColumns().size()) {
      throw new ExportOptionsValidationException(
          CoreError.DATA_LOADER_INCOMPLETE_PARTITION_KEY.buildMessage(partitionKeyNames));
    }

    // Check if the order of columns in key.getColumns() matches the order in partitionKeyNames
    Iterator<String> partitionKeyIterator = partitionKeyNames.iterator();
    for (Column<?> column : key.getColumns()) {
      // Check if the column names match in order
      if (!partitionKeyIterator.hasNext()
          || !partitionKeyIterator.next().equals(column.getName())) {
        throw new ExportOptionsValidationException(
            CoreError.DATA_LOADER_PARTITION_KEY_ORDER_MISMATCH.buildMessage(partitionKeyNames));
      }
    }
  }

  private static void validateSortOrders(
      LinkedHashSet<String> clusteringKeyNames, List<Scan.Ordering> sortOrders)
      throws ExportOptionsValidationException {
    if (sortOrders == null || sortOrders.isEmpty()) {
      return;
    }

    for (Scan.Ordering sortOrder : sortOrders) {
      checkIfColumnExistsAsClusteringKey(clusteringKeyNames, sortOrder.getColumnName());
    }
  }

  /**
   * Validates that the clustering key columns in the given Key object match the expected order
   * defined in the clusteringKeyNames. The Key can be a prefix of the clusteringKeyNames, but the
   * order must remain consistent.
   *
   * @param clusteringKeyNames the expected ordered set of clustering key names
   * @param key the Key object containing the actual clustering key columns
   * @throws ExportOptionsValidationException if the order or names of clustering keys do not match
   */
  private static void validateClusteringKey(LinkedHashSet<String> clusteringKeyNames, Key key)
      throws ExportOptionsValidationException {
    // If either clusteringKeyNames or key is null, no validation is needed
    if (clusteringKeyNames == null || key == null) {
      return;
    }

    // Create an iterator to traverse the clusteringKeyNames in order
    Iterator<String> clusteringKeyIterator = clusteringKeyNames.iterator();

    // Iterate through the columns in the given Key
    for (Column<?> column : key.getColumns()) {
      // If clusteringKeyNames have been exhausted but columns still exist in the Key,
      // it indicates a mismatch
      if (!clusteringKeyIterator.hasNext()) {
        throw new ExportOptionsValidationException(
            CoreError.DATA_LOADER_CLUSTERING_KEY_ORDER_MISMATCH.buildMessage(clusteringKeyNames));
      }

      // Get the next expected clustering key name
      String expectedKey = clusteringKeyIterator.next();

      // Check if the current column name matches the expected clustering key name
      if (!column.getName().equals(expectedKey)) {
        throw new ExportOptionsValidationException(
            CoreError.DATA_LOADER_CLUSTERING_KEY_ORDER_MISMATCH.buildMessage(clusteringKeyNames));
      }
    }
  }

  private static void checkIfColumnExistsAsClusteringKey(
      LinkedHashSet<String> clusteringKeyNames, String columnName)
      throws ExportOptionsValidationException {
    if (clusteringKeyNames == null || columnName == null) {
      return;
    }

    if (!clusteringKeyNames.contains(columnName)) {
      throw new ExportOptionsValidationException(
          CoreError.DATA_LOADER_CLUSTERING_KEY_NOT_FOUND.buildMessage(columnName));
    }
  }

  private static void validateProjectionColumns(
      LinkedHashSet<String> columnNames, List<String> columns)
      throws ExportOptionsValidationException {
    if (columns == null || columns.isEmpty()) {
      return;
    }

    for (String column : columns) {
      if (!columnNames.contains(column)) {
        throw new ExportOptionsValidationException(
            CoreError.DATA_LOADER_INVALID_PROJECTION.buildMessage(column));
      }
    }
  }
}
