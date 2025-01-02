package com.scalar.db.dataloader.core.dataimport.task.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.UnitTestUtils;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ImportSourceRecordValidatorTest {

  TableMetadata mockMetadata = UnitTestUtils.createTestTableMetadata();

  @Test
  void
      validateSourceRecord_withValidData_shouldReturnValidImportSourceRecordValidationResultWithoutErrors() {
    Set<String> partitionKeyNames = mockMetadata.getPartitionKeyNames();
    Set<String> clusteringKeyNames = mockMetadata.getClusteringKeyNames();
    Set<String> columnNames = mockMetadata.getColumnNames();
    JsonNode sourceRecord = UnitTestUtils.getOutputDataWithoutMetadata();
    ImportSourceRecordValidationResult result =
        ImportSourceRecordValidator.validateSourceRecord(
            partitionKeyNames, clusteringKeyNames, columnNames, sourceRecord, false);
    Assertions.assertTrue(result.getColumnsWithErrors().isEmpty());
  }

  @Test
  void
      validateSourceRecord_withValidDataWithAllColumnsRequired_shouldReturnValidImportSourceRecordValidationResultWithoutErrors() {
    Set<String> partitionKeyNames = mockMetadata.getPartitionKeyNames();
    Set<String> clusteringKeyNames = mockMetadata.getClusteringKeyNames();
    Set<String> columnNames = mockMetadata.getColumnNames();
    JsonNode sourceRecord = UnitTestUtils.getOutputDataWithoutMetadata();
    ImportSourceRecordValidationResult result =
        ImportSourceRecordValidator.validateSourceRecord(
            partitionKeyNames, clusteringKeyNames, columnNames, sourceRecord, true);
    Assertions.assertTrue(result.getColumnsWithErrors().isEmpty());
  }

  @Test
  void
      validateSourceRecord_withInValidPartitionKey_shouldReturnValidImportSourceRecordValidationResultWithErrors() {
    Set<String> partitionKeyNames = new HashSet<>();
    partitionKeyNames.add("id1");
    Set<String> clusteringKeyNames = mockMetadata.getClusteringKeyNames();
    Set<String> columnNames = mockMetadata.getColumnNames();
    JsonNode sourceRecord = UnitTestUtils.getOutputDataWithoutMetadata();
    ImportSourceRecordValidationResult result =
        ImportSourceRecordValidator.validateSourceRecord(
            partitionKeyNames, clusteringKeyNames, columnNames, sourceRecord, false);
    Assertions.assertFalse(result.getColumnsWithErrors().isEmpty());
  }

  @Test
  void
      validateSourceRecord_withInValidPartitionKeyWithAllColumnsRequired_shouldReturnValidImportSourceRecordValidationResultWithErrors() {
    Set<String> partitionKeyNames = new HashSet<>();
    partitionKeyNames.add("id1");
    Set<String> clusteringKeyNames = mockMetadata.getClusteringKeyNames();
    Set<String> columnNames = mockMetadata.getColumnNames();
    JsonNode sourceRecord = UnitTestUtils.getOutputDataWithoutMetadata();
    ImportSourceRecordValidationResult result =
        ImportSourceRecordValidator.validateSourceRecord(
            partitionKeyNames, clusteringKeyNames, columnNames, sourceRecord, true);
    Assertions.assertFalse(result.getColumnsWithErrors().isEmpty());
    Assertions.assertEquals(1, result.getErrorMessages().size());
  }

  @Test
  void
      validateSourceRecord_withInValidClusteringKey_shouldReturnValidImportSourceRecordValidationResultWithErrors() {
    Set<String> partitionKeyNames = mockMetadata.getPartitionKeyNames();
    Set<String> clusteringKeyNames = new HashSet<>();
    clusteringKeyNames.add("id1");
    Set<String> columnNames = mockMetadata.getColumnNames();
    JsonNode sourceRecord = UnitTestUtils.getOutputDataWithoutMetadata();
    ImportSourceRecordValidationResult result =
        ImportSourceRecordValidator.validateSourceRecord(
            partitionKeyNames, clusteringKeyNames, columnNames, sourceRecord, false);
    Assertions.assertFalse(result.getColumnsWithErrors().isEmpty());
    Assertions.assertEquals(
        "missing required field or column mapping for clustering key id1",
        result.getErrorMessages().get(0));
  }
}
