package com.scalar.db.dataloader.core.dataimport.task.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.DatabaseKeyType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ImportSourceRecordValidator {

  /**
   * Create list for validation error messages. Validate everything and not return when one single
   * error is found. Avoiding trial and error imports where every time a new error appears
   *
   * @param partitionKeyNames List of partition keys in table
   * @param clusteringKeyNames List of clustering keys in table
   * @param columnNames List of all column names in table
   * @param sourceRecord source data
   * @param allColumnsRequired If true treat missing columns as an error
   * @return Source record validation result
   */
  public static ImportSourceRecordValidationResult validateSourceRecord(
      Set<String> partitionKeyNames,
      Set<String> clusteringKeyNames,
      Set<String> columnNames,
      JsonNode sourceRecord,
      boolean allColumnsRequired,
      TableMetadata tableMetadata) {
    ImportSourceRecordValidationResult validationResult = new ImportSourceRecordValidationResult();

    // check if partition keys are found
    checkMissingKeys(DatabaseKeyType.PARTITION, partitionKeyNames, sourceRecord, validationResult);

    // check if clustering keys are found
    checkMissingKeys(
        DatabaseKeyType.CLUSTERING, clusteringKeyNames, sourceRecord, validationResult);

    // Check if the record is missing any columns
    if (allColumnsRequired) {
      checkMissingColumns(
          sourceRecord,
          columnNames,
          validationResult,
          validationResult.getColumnsWithErrors(),
          tableMetadata);
    }

    return validationResult;
  }

  /**
   * Check if the required keys are found in the data file.
   *
   * @param keyType Type of key to validate
   * @param keyColumnNames List of required column names
   * @param sourceRecord source data
   * @param validationResult Source record validation result
   */
  public static void checkMissingKeys(
      DatabaseKeyType keyType,
      Set<String> keyColumnNames,
      JsonNode sourceRecord,
      ImportSourceRecordValidationResult validationResult) {
    for (String columnName : keyColumnNames) {
      if (!sourceRecord.has(columnName)) {
        String errorMessageFormat =
            keyType == DatabaseKeyType.PARTITION
                ? CoreError.DATA_LOADER_MISSING_PARTITION_KEY_COLUMN.buildMessage(columnName)
                : CoreError.DATA_LOADER_MISSING_CLUSTERING_KEY_COLUMN.buildMessage(columnName);
        validationResult.addErrorMessage(columnName, errorMessageFormat);
      }
    }
  }

  /**
   * Make sure the json object is not missing any columns. Error added to validation errors lists
   *
   * @param sourceRecord Source json object
   * @param columnNames List of column names for a table
   * @param validationResult Source record validation result
   * @param ignoreColumns Columns that can be ignored in the check
   */
  public static void checkMissingColumns(
      JsonNode sourceRecord,
      Set<String> columnNames,
      ImportSourceRecordValidationResult validationResult,
      Set<String> ignoreColumns,
      TableMetadata tableMetadata) {
    for (String columnName : columnNames) {
      // If the field is not a metadata column and is missing and should not be ignored
      if ((ignoreColumns == null || !ignoreColumns.contains(columnName))
          && !ConsensusCommitUtils.isTransactionMetaColumn(columnName, tableMetadata)
          && !sourceRecord.has(columnName)) {
        validationResult.addErrorMessage(
            columnName, CoreError.DATA_LOADER_MISSING_COLUMN.buildMessage(columnName));
      }
    }
  }

  /**
   * Make sure the json object is not missing any columns. Error added to validation errors lists
   *
   * @param sourceRecord Source json object
   * @param columnNames List of column names for a table
   * @param validationResult Source record validation result
   */
  public static void checkMissingColumns(
      JsonNode sourceRecord,
      Set<String> columnNames,
      ImportSourceRecordValidationResult validationResult,
      TableMetadata tableMetadata) {
    ImportSourceRecordValidator.checkMissingColumns(
        sourceRecord, columnNames, validationResult, null, tableMetadata);
  }
}
