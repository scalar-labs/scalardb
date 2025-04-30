package com.scalar.db.dataloader.core.dataimport.task;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFile;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTableFieldMapping;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.dataloader.core.dataimport.processor.TableColumnDataTypes;
import com.scalar.db.dataloader.core.dataimport.task.mapping.ImportDataMapping;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResult;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResultStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.task.validation.ImportSourceRecordValidationResult;
import com.scalar.db.dataloader.core.dataimport.task.validation.ImportSourceRecordValidator;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.util.ColumnUtils;
import com.scalar.db.dataloader.core.util.KeyUtils;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/**
 * Abstract base class for handling data import tasks into ScalarDB tables. This class provides
 * functionality to import data into single or multiple tables based on the provided import options
 * and control file configurations.
 */
@SuppressWarnings({"SameNameButDifferent"})
@RequiredArgsConstructor
public abstract class ImportTask {

  protected final ImportTaskParams params;

  /**
   * Executes the import task by importing data into one or more database tables. If a control file
   * is specified in the import options, performs a multi-table import. Otherwise, performs a single
   * table import.
   *
   * @return ImportTaskResult containing the results of the import operation including
   *     success/failure status and any error messages for each target table
   */
  public ImportTaskResult execute() {

    ObjectNode mutableSourceRecord = params.getSourceRecord().deepCopy();
    ImportOptions importOptions = params.getImportOptions();

    // Single table import
    if (importOptions.getControlFile() == null) {
      String tableLookupKey =
          TableMetadataUtil.getTableLookupKey(
              importOptions.getNamespace(), importOptions.getTableName());
      ImportTargetResult singleTargetResult =
          importIntoSingleTable(
              importOptions.getNamespace(),
              importOptions.getTableName(),
              params.getTableMetadataByTableName().get(tableLookupKey),
              params.getTableColumnDataTypes().getColumnDataTypes(tableLookupKey),
              null,
              mutableSourceRecord);
      // Add the single target result to the list of targets and return the result
      return ImportTaskResult.builder()
          .rawRecord(params.getSourceRecord())
          .rowNumber(params.getRowNumber())
          .targets(Collections.singletonList(singleTargetResult))
          .build();
    }

    // Multi-table import
    List<ImportTargetResult> multiTargetResults =
        startMultiTableImportProcess(
            importOptions.getControlFile(),
            params.getTableMetadataByTableName(),
            params.getTableColumnDataTypes(),
            mutableSourceRecord);

    return ImportTaskResult.builder()
        .targets(multiTargetResults)
        .rawRecord(params.getSourceRecord())
        .rowNumber(params.getRowNumber())
        .build();
  }

  /**
   * Processes multi-table import based on the control file configuration. For each table specified
   * in the control file, validates the source data and performs the import operation.
   *
   * @param controlFile control file which maps source data columns to target table columns
   * @param tableMetadataByTableName map of table metadata indexed by table name
   * @param tableColumnDataTypes map of column data types indexed by table name
   * @param mutableSourceRecord source record data that can be modified during import
   * @return List of ImportTargetResult objects containing the results for each table import
   */
  private List<ImportTargetResult> startMultiTableImportProcess(
      ControlFile controlFile,
      Map<String, TableMetadata> tableMetadataByTableName,
      TableColumnDataTypes tableColumnDataTypes,
      ObjectNode mutableSourceRecord) {

    List<ImportTargetResult> targetResults = new ArrayList<>();

    // Import for every table mapping specified in the control file
    for (ControlFileTable controlFileTable : controlFile.getTables()) {
      for (ControlFileTableFieldMapping mapping : controlFileTable.getMappings()) {
        if (!mutableSourceRecord.has(mapping.getSourceField())
            && !mutableSourceRecord.has(mapping.getTargetColumn())) {
          String errorMessage =
              CoreError.DATA_LOADER_MISSING_SOURCE_FIELD.buildMessage(
                  mapping.getSourceField(), controlFileTable.getTable());

          ImportTargetResult targetResult =
              ImportTargetResult.builder()
                  .namespace(controlFileTable.getNamespace())
                  .tableName(controlFileTable.getTable())
                  .errors(Collections.singletonList(errorMessage))
                  .status(ImportTargetResultStatus.VALIDATION_FAILED)
                  .build();
          return Collections.singletonList(targetResult);
        }
      }

      // Import into a single table
      String tableLookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
      TableMetadata tableMetadata = tableMetadataByTableName.get(tableLookupKey);
      Map<String, DataType> dataTypesByColumns =
          tableColumnDataTypes.getColumnDataTypes(tableLookupKey);
      // Copied data to an object node data was overwritten by following operations and data check
      // fails when same object is referenced again in logic before
      ObjectNode copyNode = mutableSourceRecord.deepCopy();
      ImportTargetResult result =
          importIntoSingleTable(
              controlFileTable.getNamespace(),
              controlFileTable.getTable(),
              tableMetadata,
              dataTypesByColumns,
              controlFileTable,
              copyNode);
      targetResults.add(result);
    }
    // Wrapped in unmodifiable list to fix MixedMutabilityReturnType error-prone warning
    return Collections.unmodifiableList(targetResults);
  }

  /**
   * Imports data into a single table with validation and error handling. The method performs the
   * following steps: 1. Validates table metadata and source record 2. Creates partition and
   * clustering keys 3. Determines whether to insert or update based on existing data 4. Applies the
   * import operation according to specified import mode
   *
   * @param namespace database namespace name
   * @param table target table name
   * @param tableMetadata metadata describing the table structure
   * @param dataTypeByColumnName map of column names to their data types
   * @param controlFileTable optional control file table configuration for column mapping
   * @param mutableSourceRecord source record to be imported
   * @return ImportTargetResult containing the result of the import operation
   */
  private ImportTargetResult importIntoSingleTable(
      String namespace,
      String table,
      TableMetadata tableMetadata,
      Map<String, DataType> dataTypeByColumnName,
      ControlFileTable controlFileTable,
      ObjectNode mutableSourceRecord) {

    ImportOptions importOptions = params.getImportOptions();

    if (dataTypeByColumnName == null || tableMetadata == null) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(
              Collections.singletonList(
                  CoreError.DATA_LOADER_TABLE_METADATA_MISSING.buildMessage()))
          .build();
    }

    LinkedHashSet<String> partitionKeyNames = tableMetadata.getPartitionKeyNames();
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    LinkedHashSet<String> columnNames = tableMetadata.getColumnNames();

    applyDataMapping(controlFileTable, mutableSourceRecord);

    boolean checkForMissingColumns = shouldCheckForMissingColumns(importOptions);

    ImportSourceRecordValidationResult validationResult =
        validateSourceRecord(
            partitionKeyNames,
            clusteringKeyNames,
            columnNames,
            mutableSourceRecord,
            checkForMissingColumns,
            tableMetadata);

    if (!validationResult.isValid()) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(validationResult.getErrorMessages())
          .build();
    }

    Optional<Key> optionalPartitionKey =
        KeyUtils.createPartitionKeyFromSource(
            partitionKeyNames, dataTypeByColumnName, mutableSourceRecord);
    if (!optionalPartitionKey.isPresent()) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(
              Collections.singletonList(
                  CoreError.DATA_LOADER_COULD_NOT_FIND_PARTITION_KEY.buildMessage()))
          .build();
    }
    Optional<Key> optionalClusteringKey = Optional.empty();
    if (!clusteringKeyNames.isEmpty()) {
      optionalClusteringKey =
          KeyUtils.createClusteringKeyFromSource(
              clusteringKeyNames, dataTypeByColumnName, mutableSourceRecord);
      if (!optionalClusteringKey.isPresent()) {
        return ImportTargetResult.builder()
            .namespace(namespace)
            .tableName(table)
            .status(ImportTargetResultStatus.VALIDATION_FAILED)
            .errors(
                Collections.singletonList(
                    CoreError.DATA_LOADER_COULD_NOT_FIND_CLUSTERING_KEY.buildMessage()))
            .build();
      }
    }

    Optional<Result> optionalScalarDBResult;

    try {
      optionalScalarDBResult =
          getDataRecord(
              namespace, table, optionalPartitionKey.get(), optionalClusteringKey.orElse(null));
    } catch (ScalarDBDaoException e) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .status(ImportTargetResultStatus.RETRIEVAL_FAILED)
          .errors(Collections.singletonList(e.getMessage()))
          .build();
    }
    ImportTaskAction importAction =
        optionalScalarDBResult.isPresent() ? ImportTaskAction.UPDATE : ImportTaskAction.INSERT;

    if (importAction == ImportTaskAction.INSERT
        && shouldRevalidateMissingColumns(importOptions, checkForMissingColumns)) {
      ImportSourceRecordValidationResult validationResultForMissingColumns =
          new ImportSourceRecordValidationResult();
      ImportSourceRecordValidator.checkMissingColumns(
          mutableSourceRecord, columnNames, validationResultForMissingColumns, tableMetadata);
      if (!validationResultForMissingColumns.isValid()) {
        return ImportTargetResult.builder()
            .namespace(namespace)
            .tableName(table)
            .status(ImportTargetResultStatus.MISSING_COLUMNS)
            .errors(
                Collections.singletonList(
                    CoreError.DATA_LOADER_UPSERT_INSERT_MISSING_COLUMNS.buildMessage()))
            .build();
      }
    }

    if (shouldFailForExistingData(importAction, importOptions)) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .importedRecord(mutableSourceRecord)
          .importAction(importAction)
          .status(ImportTargetResultStatus.DATA_ALREADY_EXISTS)
          .errors(
              Collections.singletonList(CoreError.DATA_LOADER_DATA_ALREADY_EXISTS.buildMessage()))
          .build();
    }

    if (shouldFailForMissingData(importAction, importOptions)) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .importedRecord(mutableSourceRecord)
          .importAction(importAction)
          .status(ImportTargetResultStatus.DATA_NOT_FOUND)
          .errors(Collections.singletonList(CoreError.DATA_LOADER_DATA_NOT_FOUND.buildMessage()))
          .build();
    }

    List<Column<?>> columns;

    try {
      columns =
          ColumnUtils.getColumnsFromResult(
              optionalScalarDBResult.orElse(null),
              mutableSourceRecord,
              importOptions.isIgnoreNullValues(),
              tableMetadata);
    } catch (Base64Exception | ColumnParsingException e) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(Collections.singletonList(e.getMessage()))
          .build();
    }

    // Save the record
    try {
      saveRecord(
          namespace,
          table,
          optionalPartitionKey.get(),
          optionalClusteringKey.orElse(null),
          columns);

      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .importAction(importAction)
          .importedRecord(mutableSourceRecord)
          .status(ImportTargetResultStatus.SAVED)
          .build();

    } catch (ScalarDBDaoException e) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(table)
          .importAction(importAction)
          .status(ImportTargetResultStatus.SAVE_FAILED)
          .errors(Collections.singletonList(e.getMessage()))
          .build();
    }
  }

  /**
   * Applies data mapping to the given source record based on the specified control file table.
   *
   * @param controlFileTable the control file table containing column mappings
   * @param mutableSourceRecord the source record to be modified based on the mappings
   */
  private void applyDataMapping(ControlFileTable controlFileTable, ObjectNode mutableSourceRecord) {
    if (controlFileTable != null) {
      ImportDataMapping.apply(mutableSourceRecord, controlFileTable);
    }
  }

  /**
   * Determines whether missing columns should be checked based on import options.
   *
   * @param importOptions the import options to evaluate
   * @return {@code true} if missing columns should be checked, otherwise {@code false}
   */
  private boolean shouldCheckForMissingColumns(ImportOptions importOptions) {
    return importOptions.getImportMode() == ImportMode.INSERT
        || importOptions.isRequireAllColumns();
  }

  /**
   * Validates a source record against the given table metadata and constraints.
   *
   * @param partitionKeyNames the set of partition key names
   * @param clusteringKeyNames the set of clustering key names
   * @param columnNames the set of expected column names
   * @param mutableSourceRecord the source record to be validated
   * @param checkForMissingColumns whether to check for missing columns
   * @param tableMetadata the table metadata containing schema details
   * @return the validation result containing any validation errors or success status
   */
  private ImportSourceRecordValidationResult validateSourceRecord(
      LinkedHashSet<String> partitionKeyNames,
      LinkedHashSet<String> clusteringKeyNames,
      LinkedHashSet<String> columnNames,
      ObjectNode mutableSourceRecord,
      boolean checkForMissingColumns,
      TableMetadata tableMetadata) {
    return ImportSourceRecordValidator.validateSourceRecord(
        partitionKeyNames,
        clusteringKeyNames,
        columnNames,
        mutableSourceRecord,
        checkForMissingColumns,
        tableMetadata);
  }

  /**
   * Determines whether missing columns should be revalidated when performing an upsert operation.
   *
   * @param importOptions the import options to evaluate
   * @param checkForMissingColumns whether missing columns were initially checked
   * @return {@code true} if missing columns should be revalidated, otherwise {@code false}
   */
  private boolean shouldRevalidateMissingColumns(
      ImportOptions importOptions, boolean checkForMissingColumns) {
    return !checkForMissingColumns && importOptions.getImportMode() == ImportMode.UPSERT;
  }

  /**
   * Determines whether the operation should fail if data already exists.
   *
   * @param importAction the action being performed (e.g., INSERT, UPDATE)
   * @param importOptions the import options specifying behavior
   * @return {@code true} if the operation should fail for existing data, otherwise {@code false}
   */
  private boolean shouldFailForExistingData(
      ImportTaskAction importAction, ImportOptions importOptions) {
    return importAction == ImportTaskAction.UPDATE
        && importOptions.getImportMode() == ImportMode.INSERT;
  }

  /**
   * Determines whether the operation should fail if the expected data is missing.
   *
   * @param importAction the action being performed (e.g., INSERT, UPDATE)
   * @param importOptions the import options specifying behavior
   * @return {@code true} if the operation should fail for missing data, otherwise {@code false}
   */
  private boolean shouldFailForMissingData(
      ImportTaskAction importAction, ImportOptions importOptions) {
    return importAction == ImportTaskAction.INSERT
        && importOptions.getImportMode() == ImportMode.UPDATE;
  }

  /**
   * Retrieves an existing record from the database if it exists.
   *
   * @param namespace the database namespace
   * @param tableName the target table name
   * @param partitionKey the partition key for the record
   * @param clusteringKey the clustering key for the record (can be null)
   * @return Optional containing the Result if found, empty if not found
   * @throws ScalarDBDaoException if there is an error accessing the database
   */
  protected abstract Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException;

  /**
   * Saves a record to the database, either as an insert or update operation.
   *
   * @param namespace the database namespace
   * @param tableName the target table name
   * @param partitionKey the partition key for the record
   * @param clusteringKey the clustering key for the record (can be null)
   * @param columns the columns and their values to be saved
   * @throws ScalarDBDaoException if there is an error saving to the database
   */
  protected abstract void saveRecord(
      String namespace,
      String tableName,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns)
      throws ScalarDBDaoException;
}
