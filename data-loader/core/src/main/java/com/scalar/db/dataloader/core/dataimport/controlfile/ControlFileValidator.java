package com.scalar.db.dataloader.core.dataimport.controlfile;

import static com.scalar.db.dataloader.core.ErrorMessage.CONTROL_FILE_MISSING_DATA_MAPPINGS;
import static com.scalar.db.dataloader.core.ErrorMessage.DUPLICATE_DATA_MAPPINGS;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_CLUSTERING_KEY;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_COLUMN_MAPPING;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_NAMESPACE_OR_TABLE;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_PARTITION_KEY;
import static com.scalar.db.dataloader.core.ErrorMessage.MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND;
import static com.scalar.db.dataloader.core.ErrorMessage.TARGET_COLUMN_NOT_FOUND;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.util.RuntimeUtil;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Class to validate a control file */
public class ControlFileValidator {

  /**
   * Validate a control file
   *
   * @param controlFile Control file instance
   * @param controlFileValidationMode Defines the strictness of the control file validation
   * @param tableMetadataMap Metadata for one or more ScalarDB tables
   * @throws ControlFileValidationException when the control file is invalid
   */
  public static void validate(
      ControlFile controlFile,
      ControlFileValidationLevel controlFileValidationMode,
      Map<String, TableMetadata> tableMetadataMap)
      throws ControlFileValidationException {

    // Method argument null check
    RuntimeUtil.checkNotNull(controlFile, controlFileValidationMode, tableMetadataMap);

    // Make sure the control file is not empty
    checkEmptyMappings(controlFile);

    // Table metadata existence and target column validation
    Set<String> uniqueTables = new HashSet<>();
    for (ControlFileTable controlFileTable : controlFile.getTables()) {
      String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

      // Make sure that multiple table mappings for one table do not exist
      if (uniqueTables.contains(lookupKey)) {
        throw new ControlFileValidationException(String.format(DUPLICATE_DATA_MAPPINGS, lookupKey));
      }
      uniqueTables.add(lookupKey);

      // Make sure no column is mapped multiple times
      Set<String> mappedTargetColumns = getTargetColumnSet(controlFileTable);

      // Make sure table metadata is provided for each table mentioned in the data mappings
      checkMultiTableMetadata(tableMetadataMap, controlFileTable);

      TableMetadata tableMetadata = tableMetadataMap.get(lookupKey);

      // Make sure the specified target columns in the mappings actually exist
      checkIfTargetColumnExist(tableMetadata, controlFileTable);

      // Make sure all table columns are mapped
      if (controlFileValidationMode == ControlFileValidationLevel.FULL) {
        checkIfAllColumnsAreMapped(tableMetadata, mappedTargetColumns, controlFileTable);
        continue;
      }

      // Make sure all keys (partition keys and clustering keys) are mapped
      if (controlFileValidationMode == ControlFileValidationLevel.KEYS) {
        checkPartitionKeys(tableMetadata, mappedTargetColumns, controlFileTable);
        checkClusteringKeys(tableMetadata, mappedTargetColumns, controlFileTable);
      }
    }
  }

  /**
   * Check that all table columns are mapped in the control file. Ran only when the control file
   * validation mode is set to 'FULL'
   *
   * @param tableMetadata Metadata for one ScalarDB table
   * @param mappedTargetColumns All target columns that are mapped in the control file
   * @param controlFileTable Control file entry for one ScalarDB table
   * @throws ControlFileValidationException when there is a column that is not mapped in the control
   *     file
   */
  private static void checkIfAllColumnsAreMapped(
      TableMetadata tableMetadata,
      Set<String> mappedTargetColumns,
      ControlFileTable controlFileTable)
      throws ControlFileValidationException {
    LinkedHashSet<String> columnNames = tableMetadata.getColumnNames();
    for (String columnName : columnNames) {
      if (!mappedTargetColumns.contains(columnName)) {
        throw new ControlFileValidationException(
            String.format(
                MISSING_COLUMN_MAPPING,
                columnName,
                TableMetadataUtil.getTableLookupKey(controlFileTable)));
      }
    }
  }

  /**
   * Check that the control file has mappings for at least one table
   *
   * @param controlFile Control file instance
   * @throws ControlFileValidationException when the control file has no mappings for any table
   */
  private static void checkEmptyMappings(ControlFile controlFile)
      throws ControlFileValidationException {
    // Make sure data mapping for at least one table is provided
    if (controlFile.getTables().isEmpty()) {
      throw new ControlFileValidationException(CONTROL_FILE_MISSING_DATA_MAPPINGS);
    }
  }

  /**
   * Check that metadata is provided for each table that is mapped in the control file. If the table
   * metadata is missing this probably means the namespace and table combination does not exist.
   *
   * @param tableMetadataMap Metadata for one or more ScalarDB tables
   * @param controlFileTable Control file entry for one ScalarDB table
   * @throws ControlFileValidationException when metadata for a mapped table is missing
   */
  private static void checkMultiTableMetadata(
      Map<String, TableMetadata> tableMetadataMap, ControlFileTable controlFileTable)
      throws ControlFileValidationException {
    // Make sure table metadata is available for each table data mapping
    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    if (!tableMetadataMap.containsKey(lookupKey)) {
      throw new ControlFileValidationException(
          String.format(
              MISSING_NAMESPACE_OR_TABLE,
              controlFileTable.getNamespace(),
              controlFileTable.getTable()));
    }
  }

  /**
   * Check that the mapped target column exists in the provided table metadata.
   *
   * @param tableMetadata Metadata for the table
   * @param controlFileTable Control file entry for one ScalarDB table
   * @throws ControlFileValidationException when the target column does not exist
   */
  private static void checkIfTargetColumnExist(
      TableMetadata tableMetadata, ControlFileTable controlFileTable)
      throws ControlFileValidationException {

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    LinkedHashSet<String> columnNames = tableMetadata.getColumnNames();

    for (ControlFileTableFieldMapping mapping : controlFileTable.getMappings()) {
      // Make sure the target fields are found in the table metadata
      if (!columnNames.contains(mapping.getTargetColumn())) {
        throw new ControlFileValidationException(
            String.format(
                TARGET_COLUMN_NOT_FOUND,
                mapping.getTargetColumn(),
                mapping.getSourceField(),
                lookupKey));
      }
    }
  }

  /**
   * Check that the required partition keys are mapped in the control file. Ran only for control
   * file validation mode KEYS and FULL.
   *
   * @param tableMetadata Metadata for one ScalarDB table
   * @param mappedTargetColumns Set of target columns that are mapped in the control file
   * @param controlFileTable Control file entry for one ScalarDB table
   * @throws ControlFileValidationException when a partition key is not mapped
   */
  private static void checkPartitionKeys(
      TableMetadata tableMetadata,
      Set<String> mappedTargetColumns,
      ControlFileTable controlFileTable)
      throws ControlFileValidationException {
    LinkedHashSet<String> partitionKeyNames = tableMetadata.getPartitionKeyNames();
    for (String partitionKeyName : partitionKeyNames) {
      if (!mappedTargetColumns.contains(partitionKeyName)) {
        throw new ControlFileValidationException(
            String.format(
                MISSING_PARTITION_KEY,
                partitionKeyName,
                TableMetadataUtil.getTableLookupKey(controlFileTable)));
      }
    }
  }

  /**
   * Check that the required clustering keys are mapped in the control file. Ran only for control
   * file validation mode KEYS and FULL.
   *
   * @param tableMetadata Metadata for one ScalarDB table
   * @param mappedTargetColumns Set of target columns that are mapped in the control file
   * @param controlFileTable Control file entry for one ScalarDB table
   * @throws ControlFileValidationException when a clustering key is not mapped
   */
  private static void checkClusteringKeys(
      TableMetadata tableMetadata,
      Set<String> mappedTargetColumns,
      ControlFileTable controlFileTable)
      throws ControlFileValidationException {
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    for (String clusteringKeyName : clusteringKeyNames) {
      if (!mappedTargetColumns.contains(clusteringKeyName)) {
        throw new ControlFileValidationException(
            String.format(
                MISSING_CLUSTERING_KEY,
                clusteringKeyName,
                TableMetadataUtil.getTableLookupKey(controlFileTable)));
      }
    }
  }

  /**
   * Check that a control file table mapping does not contain duplicate mappings for the same target
   * column and creates a set of unique mappings
   *
   * @param controlFileTable Control file entry for one ScalarDB table
   * @return Set of uniquely mapped target columns
   * @throws ControlFileValidationException when a duplicate mapping is found
   */
  private static Set<String> getTargetColumnSet(ControlFileTable controlFileTable)
      throws ControlFileValidationException {
    Set<String> mappedTargetColumns = new HashSet<>();
    for (ControlFileTableFieldMapping mapping : controlFileTable.getMappings()) {
      if (!mappedTargetColumns.add(mapping.getTargetColumn())) {
        throw new ControlFileValidationException(
            String.format(
                MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND,
                mapping.getTargetColumn(),
                TableMetadataUtil.getTableLookupKey(controlFileTable)));
      }
    }
    return mappedTargetColumns;
  }
}
