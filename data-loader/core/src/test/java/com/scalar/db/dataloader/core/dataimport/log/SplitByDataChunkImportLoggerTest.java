package com.scalar.db.dataloader.core.dataimport.log;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.writer.DefaultLogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactoryConfig;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitByDataChunkImportLoggerTest {

  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();

  @TempDir Path tempDir;

  private LogWriterFactory logWriterFactory;

  @BeforeEach
  void setUp() {
    LogWriterFactoryConfig logWriterFactoryConfig =
        LogWriterFactoryConfig.builder()
            .logStorageLocation(LogStorageLocation.LOCAL_FILE_STORAGE)
            .build();
    ImportLoggerConfig importLoggerConfig =
        ImportLoggerConfig.builder()
            .prettyPrint(false)
            .logSuccessRecords(false)
            .logRawSourceRecords(false)
            .logDirectoryPath("path")
            .build();
    logWriterFactory = new DefaultLogWriterFactory(logWriterFactoryConfig, importLoggerConfig);
  }

  @Test
  void onTransactionBatchCompleted_NoErrors_ShouldWriteToDataChunkSuccessFiles()
      throws IOException {
    testTransactionBatchCompleted(true, true);
  }

  @Test
  void onTransactionBatchCompleted_HasErrors_ShouldWriteToDataChunkFailureFiles()
      throws IOException {
    testTransactionBatchCompleted(false, true);
  }

  @Test
  void onTransactionBatchCompleted_NoErrorsAndNoSuccessFileLogging_ShouldNotWriteToSuccessFiles()
      throws IOException {
    testTransactionBatchCompleted(true, false);
  }

  private void testTransactionBatchCompleted(boolean success, boolean logSuccessRecords)
      throws IOException {
    // Arrange
    ImportLoggerConfig config =
        ImportLoggerConfig.builder()
            .logDirectoryPath(tempDir.toString() + "/")
            .logRawSourceRecords(true)
            .logSuccessRecords(logSuccessRecords)
            .build();
    SplitByDataChunkImportLogger importLogger =
        new SplitByDataChunkImportLogger(config, logWriterFactory);

    List<ImportTransactionBatchResult> batchResults = new ArrayList<>();

    for (int i = 1; i <= 3; i++) {
      List<ImportTaskResult> records =
          Collections.singletonList(
              ImportTaskResult.builder()
                  .rowNumber(i)
                  .targets(Collections.EMPTY_LIST)
                  .rawRecord(OBJECT_MAPPER.createObjectNode())
                  .build());
      ImportTransactionBatchResult result =
          ImportTransactionBatchResult.builder()
              .dataChunkId(i)
              .transactionBatchId(1)
              .records(records)
              .success(success)
              .build();
      batchResults.add(result);
    }

    // Act
    for (ImportTransactionBatchResult batchResult : batchResults) {
      importLogger.onTransactionBatchCompleted(batchResult);
      importLogger.onDataChunkCompleted(
          ImportDataChunkStatus.builder().dataChunkId(batchResult.getDataChunkId()).build());
    }
    importLogger.onAllDataChunksCompleted();

    // Assert
    for (int i = 0; i < batchResults.size(); i++) {
      ImportTransactionBatchResult batchResult = batchResults.get(i);
      String logFileNameFormat =
          success
              ? SplitByDataChunkImportLogger.SUCCESS_LOG_FILE_NAME_FORMAT
              : SplitByDataChunkImportLogger.FAILURE_LOG_FILE_NAME_FORMAT;
      Path dataChunkLogFileName = tempDir.resolve(String.format(logFileNameFormat, i + 1));

      if (success && logSuccessRecords) {
        assertTrue(Files.exists(dataChunkLogFileName), "Data chunk success log file should exist");
        assertTransactionBatchResult(batchResult, dataChunkLogFileName);
      } else if (!success) {
        assertTrue(Files.exists(dataChunkLogFileName), "Data chunk failure log file should exist");
        assertTransactionBatchResult(batchResult, dataChunkLogFileName);
      } else {
        assertFalse(
            Files.exists(dataChunkLogFileName), "Data chunk success log file should not exist");
      }
    }
  }

  private void assertTransactionBatchResult(
      ImportTransactionBatchResult expected, Path dataChunkLogFileName) throws IOException {
    //    String logContent = Files.readString(dataChunkLogFileName);
    String logContent =
        new String(Files.readAllBytes(dataChunkLogFileName), StandardCharsets.UTF_8);
    DataLoaderObjectMapper objectMapper = new DataLoaderObjectMapper();
    List<ImportTransactionBatchResult> logEntries =
        objectMapper.readValue(
            logContent, new TypeReference<List<ImportTransactionBatchResult>>() {});
    ImportTransactionBatchResult actual = logEntries.get(0);

    assertEquals(expected.getDataChunkId(), actual.getDataChunkId(), "Data chunk ID should match");
    assertEquals(
        expected.getTransactionBatchId(),
        actual.getTransactionBatchId(),
        "Transaction batch ID should match");
    assertEquals(
        expected.getTransactionId(), actual.getTransactionId(), "Transaction ID should match");
    assertEquals(expected.isSuccess(), actual.isSuccess(), "Success status should match");

    List<ImportTaskResult> expectedRecords = expected.getRecords();
    List<ImportTaskResult> actualRecords = actual.getRecords();
    assertEquals(expectedRecords.size(), actualRecords.size(), "Number of records should match");
    for (int j = 0; j < expectedRecords.size(); j++) {
      ImportTaskResult expectedRecord = expectedRecords.get(j);
      ImportTaskResult actualRecord = actualRecords.get(j);
      assertEquals(
          expectedRecord.getRowNumber(), actualRecord.getRowNumber(), "Row number should match");
      assertEquals(
          expectedRecord.getRawRecord(), actualRecord.getRawRecord(), "Raw record should match");
      assertEquals(expectedRecord.getTargets(), actualRecord.getTargets(), "Targets should match");
    }
  }

  @Test
  void onDataChunkCompleted_NoErrors_ShouldWriteToSummaryLogFile() throws IOException {
    testDataChunkCompleted(
        String.format(SplitByDataChunkImportLogger.SUMMARY_LOG_FILE_NAME_FORMAT, "%d"), false);
  }

  @Test
  void onDataChunkCompleted_HasErrors_ShouldWriteToSummaryLogFile() throws IOException {
    testDataChunkCompleted(
        String.format(SplitByDataChunkImportLogger.SUMMARY_LOG_FILE_NAME_FORMAT, "%d"), true);
  }

  private void testDataChunkCompleted(String logFilePattern, boolean hasErrors) throws IOException {
    ImportLoggerConfig config =
        ImportLoggerConfig.builder()
            .logDirectoryPath(tempDir.toString() + "/")
            .logRawSourceRecords(true)
            .logSuccessRecords(true)
            .build();
    SplitByDataChunkImportLogger importLogger =
        new SplitByDataChunkImportLogger(config, logWriterFactory);

    List<ImportDataChunkStatus> dataChunkStatuses =
        IntStream.rangeClosed(1, 2)
            .mapToObj(id -> createDataChunkStatus(id, hasErrors))
            .collect(Collectors.toList());

    dataChunkStatuses.forEach(importLogger::onDataChunkCompleted);
    importLogger.onAllDataChunksCompleted();

    assertDataChunkStatusLog(logFilePattern, dataChunkStatuses);
  }

  private ImportDataChunkStatus createDataChunkStatus(int dataChunkId, boolean hasErrors) {
    return ImportDataChunkStatus.builder()
        .dataChunkId(dataChunkId)
        .startTime(Instant.now())
        .endTime(Instant.now())
        .totalRecords(100)
        .successCount(hasErrors ? 90 : 100)
        .failureCount(hasErrors ? 10 : 0)
        .batchCount(5)
        .totalDurationInMilliSeconds(1000)
        .build();
  }

  private void assertDataChunkStatusLog(
      String logFilePattern, List<ImportDataChunkStatus> dataChunkStatuses) throws IOException {
    for (ImportDataChunkStatus dataChunkStatus : dataChunkStatuses) {
      String logFileName = String.format(logFilePattern, dataChunkStatus.getDataChunkId());
      Path dataChunkLogFile = tempDir.resolve(logFileName);
      assertTrue(Files.exists(dataChunkLogFile), "Data chunk summary log file should exist");

      //      String logContent = Files.readString(dataChunkLogFile);
      String logContent = new String(Files.readAllBytes(dataChunkLogFile), StandardCharsets.UTF_8);
      DataLoaderObjectMapper objectMapper = new DataLoaderObjectMapper();
      List<ImportDataChunkStatus> logEntries =
          objectMapper.readValue(logContent, new TypeReference<List<ImportDataChunkStatus>>() {});

      assertEquals(1, logEntries.size());
      assertDataChunkStatusEquals(dataChunkStatus, logEntries.get(0));
    }
  }

  private void assertDataChunkStatusEquals(
      ImportDataChunkStatus expected, ImportDataChunkStatus actual) {
    assertEquals(expected.getDataChunkId(), actual.getDataChunkId());
    assertEquals(expected.getStartTime(), actual.getStartTime());
    assertEquals(expected.getEndTime(), actual.getEndTime());
    assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
    assertEquals(expected.getFailureCount(), actual.getFailureCount());
    assertEquals(expected.getBatchCount(), actual.getBatchCount());
    assertEquals(
        expected.getTotalDurationInMilliSeconds(), actual.getTotalDurationInMilliSeconds());
  }
}
