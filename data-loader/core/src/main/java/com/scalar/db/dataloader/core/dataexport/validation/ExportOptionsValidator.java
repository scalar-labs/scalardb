package com.scalar.db.dataloader.core.dataexport.validation;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.io.Key;
import java.util.LinkedHashSet;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A validator for ensuring that export options are consistent with the ScalarDB table metadata and
 * follow the defined constraints.
 */
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
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    ScanRange scanRange = exportOptions.getScanRange();

    // Validate projection columns
    validateProjectionColumns(tableMetadata.getColumnNames(), exportOptions.getProjectionColumns());

    // Validate sort orders
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
   * Checks if the provided clustering key is valid for the ScalarDB table.
   *
   * @param clusteringKeyNames The set of valid clustering key names for the table.
   * @param key The ScalarDB key to validate.
   * @throws ExportOptionsValidationException If the key is invalid or not a clustering key.
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
   * Checks if the provided clustering key column name is valid for the ScalarDB table.
   *
   * @param clusteringKeyNames The set of valid clustering key names for the table.
   * @param columnName The column name of the clustering key to validate.
   * @throws ExportOptionsValidationException If the column name is not a valid clustering key.
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
   * Checks if the provided projection column names are valid for the ScalarDB table.
   *
   * @param columnNames The set of valid column names for the table.
   * @param columns The list of projection column names to validate.
   * @throws ExportOptionsValidationException If any of the column names are invalid.
   */
  private static void validateProjectionColumns(
      LinkedHashSet<String> columnNames, List<String> columns)
      throws ExportOptionsValidationException {
    if (columns == null || columns.isEmpty()) {
      return;
    }
    for (String column : columns) {
      // O(n) lookup, but acceptable given the typically small list size
      if (!columnNames.contains(column)) {
        throw new ExportOptionsValidationException(
            CoreError.DATA_LOADER_INVALID_PROJECTION.buildMessage(column));
      }
    }
  }
}
