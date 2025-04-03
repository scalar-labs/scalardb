package com.scalar.db.dataloader.core.dataimport.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.writer.DefaultLogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactoryConfig;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SingleFileImportLoggerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileImportLoggerTest.class);
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

  @AfterEach
  void tearDown() throws IOException {
    cleanUpTempDir();
  }

  private void cleanUpTempDir() throws IOException {
    try (Stream<Path> paths = Files.list(tempDir)) {
      paths.forEach(this::deleteFile);
    }
  }

  private void deleteFile(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      LOGGER.error("Failed to delete file: {}", file, e);
    }
  }

  @Test
  void onTransactionBatchCompleted_NoErrors_ShouldWriteToSuccessLogFile() throws IOException {
    testTransactionBatchCompleted(true, true);
  }

  @Test
  void onTransactionBatchCompleted_HasErrors_ShouldWriteToFailureLogFile() throws IOException {
    testTransactionBatchCompleted(false, true);
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
    SingleFileImportLogger importLogger = new SingleFileImportLogger(config, logWriterFactory);

    List<ImportTransactionBatchResult> batchResults = createBatchResults(1, success);

    // Act
    for (ImportTransactionBatchResult batchResult : batchResults) {
      importLogger.onTransactionBatchCompleted(batchResult);
      importLogger.onDataChunkCompleted(
          ImportDataChunkStatus.builder().dataChunkId(batchResult.getDataChunkId()).build());
    }
    importLogger.onAllDataChunksCompleted();

    // Assert
    assertTransactionBatchResults(batchResults, success, logSuccessRecords);
  }

  private List<ImportTransactionBatchResult> createBatchResults(int count, boolean success) {
    List<ImportTransactionBatchResult> batchResults = new ArrayList<>();

    for (int i = 1; i <= count; i++) {
      List<ImportTaskResult> records =
          Collections.singletonList(
              ImportTaskResult.builder()
                  .rowNumber(i)
                  .rawRecord(OBJECT_MAPPER.createObjectNode())
                  .targets(Collections.EMPTY_LIST)
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

    return batchResults;
  }

  private void assertTransactionBatchResults(
      List<ImportTransactionBatchResult> batchResults, boolean success, boolean logSuccessRecords)
      throws IOException {
    DataLoaderObjectMapper objectMapper = new DataLoaderObjectMapper();

    // Single file log mode
    Path logFileName =
        tempDir.resolve(
            success
                ? SingleFileImportLogger.SUCCESS_LOG_FILE_NAME
                : SingleFileImportLogger.FAILURE_LOG_FILE_NAME);
    if (logSuccessRecords || !success) {
      assertTrue(Files.exists(logFileName), "Log file should exist");

      String logContent = new String(Files.readAllBytes(logFileName), StandardCharsets.UTF_8);

      List<ImportTransactionBatchResult> logEntries =
          objectMapper.readValue(
              logContent, new TypeReference<List<ImportTransactionBatchResult>>() {});

      assertEquals(
          batchResults.size(),
          logEntries.size(),
          "Number of log entries should match the number of batch results");

      for (int i = 0; i < batchResults.size(); i++) {
        assertTransactionBatchResult(batchResults.get(i), logEntries.get(i));
      }
    } else {
      assertFalse(Files.exists(logFileName), "Log file should not exist");
    }
  }

  private void assertTransactionBatchResult(
      ImportTransactionBatchResult expected, ImportTransactionBatchResult actual) {
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
    testDataChunkCompleted(false);
  }

  @Test
  void onDataChunkCompleted_HasErrors_ShouldWriteToSummaryLogFile() throws IOException {
    testDataChunkCompleted(true);
  }

  private void testDataChunkCompleted(boolean hasErrors) throws IOException {
    ImportLoggerConfig config =
        ImportLoggerConfig.builder()
            .logDirectoryPath(tempDir.toString() + "/")
            .logRawSourceRecords(true)
            .logSuccessRecords(true)
            .build();
    SingleFileImportLogger importLogger = new SingleFileImportLogger(config, logWriterFactory);

    List<ImportDataChunkStatus> dataChunkStatuses =
        Stream.of(1, 2)
            .map(id -> createDataChunkStatus(id, hasErrors))
            .collect(Collectors.toList());

    dataChunkStatuses.forEach(importLogger::onDataChunkCompleted);
    importLogger.onAllDataChunksCompleted();

    assertDataChunkStatusLog(SingleFileImportLogger.SUMMARY_LOG_FILE_NAME, dataChunkStatuses);
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
    assertSingleFileLog(tempDir, logFilePattern, dataChunkStatuses);
  }

  private void assertSingleFileLog(
      Path tempDir, String logFileName, List<ImportDataChunkStatus> dataChunkStatuses)
      throws IOException {
    Path summaryLogFile = tempDir.resolve(logFileName);
    assertTrue(Files.exists(summaryLogFile));

    String logContent = new String(Files.readAllBytes(summaryLogFile), StandardCharsets.UTF_8);
    DataLoaderObjectMapper objectMapper = new DataLoaderObjectMapper();
    List<ImportDataChunkStatus> logEntries =
        objectMapper.readValue(logContent, new TypeReference<List<ImportDataChunkStatus>>() {});

    assertEquals(dataChunkStatuses.size(), logEntries.size());
    for (int i = 0; i < dataChunkStatuses.size(); i++) {
      assertDataChunkStatusEquals(dataChunkStatuses.get(i), logEntries.get(i));
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
