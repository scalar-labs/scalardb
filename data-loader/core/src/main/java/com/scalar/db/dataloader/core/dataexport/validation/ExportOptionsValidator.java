package com.scalar.db.dataloader.core.dataexport.validation;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.io.Key;
import java.util.LinkedHashSet;
import java.util.List;

/** Validation for Export options */
public class ExportOptionsValidator {

  /**
   * Validate export request
   *
   * @param exportOptions Export options
   * @param tableMetadata Metadata for a single ScalarDB table
   * @throws ExportOptionsValidationException when the options are invalid
   */
  public static void validate(ExportOptions exportOptions, TableMetadata tableMetadata)
      throws ExportOptionsValidationException {
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    ScanRange scanRange = exportOptions.getScanRange();

    // validate projections
    validateProjectionColumns(tableMetadata.getColumnNames(), exportOptions.getProjectionColumns());

    // validate sorts
    if (!exportOptions.getSortOrders().isEmpty()) {
      for (Scan.Ordering sort : exportOptions.getSortOrders()) {
        validateClusteringKey(clusteringKeyNames, sort.getColumnName());
      }
    }

    // Validate scan start key
    if (scanRange.getScanStartKey() != null) {
      validateClusteringKey(clusteringKeyNames, scanRange.getScanStartKey());
    }

    // Validate scan end key
    if (scanRange.getScanEndKey() != null) {
      validateClusteringKey(clusteringKeyNames, scanRange.getScanEndKey());
    }
  }

  /**
   * Check if the provided clustering key is available in the ScalarDB table
   *
   * @param clusteringKeyNames List of clustering key names available in a
   * @param key To be validated ScalarDB key
   * @throws ExportOptionsValidationException if the key could not be found or is not a clustering
   *     key
   */
  private static void validateClusteringKey(LinkedHashSet<String> clusteringKeyNames, Key key)
      throws ExportOptionsValidationException {
    if (clusteringKeyNames == null) {
      return;
    }
    String columnName = key.getColumnName(0);
    validateClusteringKey(clusteringKeyNames, columnName);
  }

  /**
   * Check if the provided clustering key is available in the ScalarDB table
   *
   * @param clusteringKeyNames List of clustering key names available in a
   * @param columnName Column name of the to be validated clustering key
   * @throws ExportOptionsValidationException if the key could not be found or is not a clustering
   *     key
   */
  private static void validateClusteringKey(
      LinkedHashSet<String> clusteringKeyNames, String columnName)
      throws ExportOptionsValidationException {
    if (clusteringKeyNames == null) {
      return;
    }

    if (!clusteringKeyNames.contains(columnName)) {
      throw new ExportOptionsValidationException(
          CoreError.DATA_LOADER_CLUSTERING_KEY_NOT_FOUND.buildMessage(columnName));
    }
  }

  /**
   * Check if the provided projection column names are available in the ScalarDB table
   *
   * @param columnNames List of ScalarDB table column names
   * @param columns List of to be validated column names
   * @throws ExportOptionsValidationException if the column name was not found in the table
   */
  private static void validateProjectionColumns(
      LinkedHashSet<String> columnNames, List<String> columns)
      throws ExportOptionsValidationException {
    if (columns == null || columns.isEmpty()) {
      return;
    }
    for (String column : columns) {
      // O(n) but list is always going to be very small, so it's ok
      if (!columnNames.contains(column)) {
        throw new ExportOptionsValidationException(
            CoreError.DATA_LOADER_INVALID_PROJECTION.buildMessage(column));
      }
    }
  }
}
