package com.scalar.db.dataloader.core.dataimport.task;

import static com.scalar.db.dataloader.core.dataimport.task.ImportTaskConstants.*;

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

@RequiredArgsConstructor
public abstract class ImportTask {

  protected final ImportTaskParams params;

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
                  mapping.getSourceField(), controlFileTable.getTableName());

          ImportTargetResult targetResult =
              ImportTargetResult.builder()
                  .namespace(controlFileTable.getNamespace())
                  .tableName(controlFileTable.getTableName())
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
              controlFileTable.getTableName(),
              tableMetadata,
              dataTypesByColumns,
              controlFileTable,
              copyNode);
      targetResults.add(result);
    }
    return targetResults;
  }

  private ImportTargetResult importIntoSingleTable(
      String namespace,
      String tableName,
      TableMetadata tableMetadata,
      Map<String, DataType> dataTypeByColumnName,
      ControlFileTable controlFileTable,
      ObjectNode mutableSourceRecord) {

    ImportOptions importOptions = params.getImportOptions();

    if (dataTypeByColumnName == null || tableMetadata == null) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(Collections.singletonList(ERROR_TABLE_METADATA_MISSING))
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
            checkForMissingColumns);

    if (!validationResult.isValid()) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
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
          .tableName(tableName)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(Collections.singletonList(ERROR_COULD_NOT_FIND_PARTITION_KEY))
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
            .tableName(tableName)
            .status(ImportTargetResultStatus.VALIDATION_FAILED)
            .errors(Collections.singletonList(ERROR_COULD_NOT_FIND_CLUSTERING_KEY))
            .build();
      }
    }

    Optional<Result> optionalScalarDBResult;

    try {
      optionalScalarDBResult =
          getDataRecord(
              namespace, tableName, optionalPartitionKey.get(), optionalClusteringKey.orElse(null));
    } catch (ScalarDBDaoException e) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
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
          mutableSourceRecord, columnNames, validationResultForMissingColumns);
      if (!validationResultForMissingColumns.isValid()) {
        return ImportTargetResult.builder()
            .namespace(namespace)
            .tableName(tableName)
            .status(ImportTargetResultStatus.MISSING_COLUMNS)
            .errors(Collections.singletonList(ERROR_UPSERT_INSERT_MISSING_COLUMNS))
            .build();
      }
    }

    if (shouldFailForExistingData(importAction, importOptions)) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
          .importedRecord(mutableSourceRecord)
          .importAction(importAction)
          .status(ImportTargetResultStatus.DATA_ALREADY_EXISTS)
          .errors(Collections.singletonList(ERROR_DATA_ALREADY_EXISTS))
          .build();
    }

    if (shouldFailForMissingData(importAction, importOptions)) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
          .importedRecord(mutableSourceRecord)
          .importAction(importAction)
          .status(ImportTargetResultStatus.DATA_NOT_FOUND)
          .errors(Collections.singletonList(ERROR_DATA_NOT_FOUND))
          .build();
    }

    List<Column<?>> columns;

    try {
      columns =
          ColumnUtils.getColumnsFromResult(
              optionalScalarDBResult.orElse(null),
              mutableSourceRecord,
              importOptions.isIgnoreNullValues(),
              partitionKeyNames,
              clusteringKeyNames,
              columnNames,
              dataTypeByColumnName);
    } catch (Base64Exception | ColumnParsingException e) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
          .status(ImportTargetResultStatus.VALIDATION_FAILED)
          .errors(Collections.singletonList(e.getMessage()))
          .build();
    }

    // Time to save the record
    try {
      saveRecord(
          namespace,
          tableName,
          optionalPartitionKey.get(),
          optionalClusteringKey.orElse(null),
          columns);

      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
          .importAction(importAction)
          .importedRecord(mutableSourceRecord)
          .status(ImportTargetResultStatus.SAVED)
          .build();

    } catch (ScalarDBDaoException e) {
      return ImportTargetResult.builder()
          .namespace(namespace)
          .tableName(tableName)
          .importAction(importAction)
          .status(ImportTargetResultStatus.SAVE_FAILED)
          .errors(Collections.singletonList(e.getMessage()))
          .build();
    }
  }

  private void applyDataMapping(ControlFileTable controlFileTable, ObjectNode mutableSourceRecord) {
    if (controlFileTable != null) {
      ImportDataMapping.apply(mutableSourceRecord, controlFileTable);
    }
  }

  private boolean shouldCheckForMissingColumns(ImportOptions importOptions) {
    return importOptions.getImportMode() == ImportMode.INSERT
        || importOptions.isRequireAllColumns();
  }

  private ImportSourceRecordValidationResult validateSourceRecord(
      LinkedHashSet<String> partitionKeyNames,
      LinkedHashSet<String> clusteringKeyNames,
      LinkedHashSet<String> columnNames,
      ObjectNode mutableSourceRecord,
      boolean checkForMissingColumns) {
    return ImportSourceRecordValidator.validateSourceRecord(
        partitionKeyNames,
        clusteringKeyNames,
        columnNames,
        mutableSourceRecord,
        checkForMissingColumns);
  }

  private boolean shouldRevalidateMissingColumns(
      ImportOptions importOptions, boolean checkForMissingColumns) {
    return !checkForMissingColumns && importOptions.getImportMode() == ImportMode.UPSERT;
  }

  private boolean shouldFailForExistingData(
      ImportTaskAction importAction, ImportOptions importOptions) {
    return importAction == ImportTaskAction.UPDATE
        && importOptions.getImportMode() == ImportMode.INSERT;
  }

  private boolean shouldFailForMissingData(
      ImportTaskAction importAction, ImportOptions importOptions) {
    return importAction == ImportTaskAction.INSERT
        && importOptions.getImportMode() == ImportMode.UPDATE;
  }

  protected abstract Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException;

  protected abstract void saveRecord(
      String namespace,
      String tableName,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns)
      throws ScalarDBDaoException;
}
