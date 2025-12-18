package com.scalar.db.dataloader.core.dataimport.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.TransactionMode;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.ImportRow;
import com.scalar.db.dataloader.core.dataimport.ImportStatus;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import com.scalar.db.exception.transaction.TransactionException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for the ImportProcessor class.
 *
 * <p>These tests verify that the process method correctly handles different scenarios including
 * single CRUD mode, consensus commit mode, empty data, and streaming batch processing.
 */
@SuppressWarnings("SameNameButDifferent")
@ExtendWith(MockitoExtension.class)
class ImportProcessorTest {

  @Mock private ImportProcessorParams params;
  @Mock private ImportOptions importOptions;
  @Mock private ScalarDbDao dao;
  @Mock private DistributedTransactionManager distributedTransactionManager;
  @Mock private DistributedTransaction distributedTransaction;
  @Mock private TableColumnDataTypes tableColumnDataTypes;
  @Mock private ImportEventListener eventListener;

  private Map<String, TableMetadata> tableMetadataByTableName;

  @BeforeEach
  void setUp() {
    // Only set up the common mocks that are used by all tests
    tableMetadataByTableName = new HashMap<>();
    tableMetadataByTableName.put("namespace.table", UnitTestUtils.createTestTableMetadata());
  }

  @Test
  void process_withSingleCrudMode_shouldProcessAllRecords() {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader("test data"));
    when(params.getTransactionMode()).thenReturn(TransactionMode.SINGLE_CRUD);
    when(params.getDao()).thenReturn(dao);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);
    when(params.getImportOptions()).thenReturn(importOptions);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(1, reader);

    // Assert
    verify(eventListener, times(1)).onAllImportsCompleted();
    // Verify that import was processed
    verify(eventListener, times(1)).onImportCompleted(any(ImportStatus.class));
  }

  @Test
  void process_withConsensusCommitMode_shouldProcessAllRecords() throws TransactionException {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader("test data"));
    when(params.getTransactionMode()).thenReturn(TransactionMode.CONSENSUS_COMMIT);
    when(params.getDao()).thenReturn(dao);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);
    when(params.getImportOptions()).thenReturn(importOptions);
    when(distributedTransactionManager.start()).thenReturn(distributedTransaction);

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(1, reader);

    // Assert
    verify(eventListener, times(1)).onAllImportsCompleted();
    // Verify that import was processed
    verify(eventListener, times(1)).onImportCompleted(any(ImportStatus.class));
  }

  @Test
  void process_withEmptyData_shouldNotProcessAnyRecords() {
    // Arrange
    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);
    processor.setReturnEmptyBatch(true);

    BufferedReader reader = new BufferedReader(new StringReader(""));

    // Act
    processor.process(1, reader);

    // Assert
    verify(eventListener, times(1)).onAllImportsCompleted();
    // Import events are still fired once
    verify(eventListener, times(1)).onImportStarted(any());
    verify(eventListener, times(1)).onImportCompleted(any());
  }

  @Test
  void process_withMultipleRecords_shouldProcessInBatches() throws TransactionException {
    // Arrange
    final int batchSize = 3;
    when(params.getTransactionMode()).thenReturn(TransactionMode.CONSENSUS_COMMIT);
    when(params.getDao()).thenReturn(dao);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);
    when(params.getImportOptions()).thenReturn(importOptions);
    when(distributedTransactionManager.start()).thenReturn(distributedTransaction);

    // Create test data with 10 lines
    StringBuilder testData = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      testData.append("test data line ").append(i).append("\n");
    }
    BufferedReader reader = new BufferedReader(new StringReader(testData.toString()));

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(batchSize, reader);

    // Assert
    // With 10 records and batch size 3, we expect 4 batches (3+3+3+1)
    verify(eventListener, times(4))
        .onTransactionBatchStarted(any(ImportTransactionBatchStatus.class));
    verify(eventListener, times(4))
        .onTransactionBatchCompleted(any(ImportTransactionBatchResult.class));
    verify(eventListener, times(1)).onAllImportsCompleted();

    // Verify all 10 records were processed
    assertEquals(10, processor.getTotalRecordsProcessed().get());
  }

  @Test
  void process_withSingleCrudAndMultipleRecords_shouldProcessEachRecordIndividually() {
    // Arrange
    when(params.getTransactionMode()).thenReturn(TransactionMode.SINGLE_CRUD);
    when(params.getDao()).thenReturn(dao);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);
    when(params.getImportOptions()).thenReturn(importOptions);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);

    // Create test data with 5 lines
    StringBuilder testData = new StringBuilder();
    for (int i = 0; i < 5; i++) {
      testData.append("test data line ").append(i).append("\n");
    }
    BufferedReader reader = new BufferedReader(new StringReader(testData.toString()));

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(2, reader);

    // Assert
    verify(eventListener, times(1)).onAllImportsCompleted();
    // Verify all 5 records were processed
    assertEquals(5, processor.getTotalRecordsProcessed().get());
  }

  @Test
  void process_withUnexpectedExceptionInTransaction_shouldHandleGracefully()
      throws TransactionException {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader("test data"));
    when(params.getTransactionMode()).thenReturn(TransactionMode.CONSENSUS_COMMIT);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);
    when(distributedTransactionManager.start()).thenThrow(new RuntimeException("Unexpected error"));

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(1, reader);

    // Assert
    verify(eventListener, times(1)).onAllImportsCompleted();

    // Capture and verify the transaction batch result
    ArgumentCaptor<ImportTransactionBatchResult> resultCaptor =
        ArgumentCaptor.forClass(ImportTransactionBatchResult.class);
    verify(eventListener, times(1)).onTransactionBatchCompleted(resultCaptor.capture());

    ImportTransactionBatchResult result = resultCaptor.getValue();
    assertFalse(result.isSuccess());
    assertEquals(0, result.getTransactionBatchId());
    assertTrue(result.getErrors().get(0).contains("Unexpected error"));
  }

  @Test
  void process_withStreamingBatches_shouldMaintainCorrectBatchIds() throws TransactionException {
    // Arrange
    final int batchSize = 2;
    when(params.getTransactionMode()).thenReturn(TransactionMode.CONSENSUS_COMMIT);
    when(params.getDao()).thenReturn(dao);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);
    when(params.getImportOptions()).thenReturn(importOptions);
    when(distributedTransactionManager.start()).thenReturn(distributedTransaction);

    // Create test data with 5 lines (will create 3 batches: 2+2+1)
    StringBuilder testData = new StringBuilder();
    for (int i = 0; i < 5; i++) {
      testData.append("test data line ").append(i).append("\n");
    }
    BufferedReader reader = new BufferedReader(new StringReader(testData.toString()));

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(batchSize, reader);

    // Assert
    ArgumentCaptor<ImportTransactionBatchResult> resultCaptor =
        ArgumentCaptor.forClass(ImportTransactionBatchResult.class);
    verify(eventListener, times(3)).onTransactionBatchCompleted(resultCaptor.capture());

    List<ImportTransactionBatchResult> results = resultCaptor.getAllValues();
    // Verify batch IDs are sequential
    assertEquals(0, results.get(0).getTransactionBatchId());
    assertEquals(1, results.get(1).getTransactionBatchId());
    assertEquals(2, results.get(2).getTransactionBatchId());
  }

  /**
   * A simple implementation of ImportProcessor for testing purposes. This class implements the
   * streaming readNextBatch method for testing.
   */
  static class TestImportProcessor extends ImportProcessor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Getter private final AtomicInteger totalRecordsProcessed = new AtomicInteger(0);
    private final List<String> lines = new ArrayList<>();
    private int currentLineIndex = 0;
    private int currentRowNumber = 1;
    private boolean linesLoaded = false;
    private boolean returnEmptyBatch = false;

    public TestImportProcessor(ImportProcessorParams params) {
      super(params);
    }

    public void setReturnEmptyBatch(boolean value) {
      this.returnEmptyBatch = value;
    }

    @Override
    protected List<ImportRow> readNextBatch(BufferedReader reader, int batchSize) {
      List<ImportRow> batch = new ArrayList<>();

      if (returnEmptyBatch) {
        return batch;
      }

      try {
        // Load all lines on first call
        if (!linesLoaded) {
          String line;
          while ((line = reader.readLine()) != null) {
            if (!line.trim().isEmpty()) {
              lines.add(line);
            }
          }
          linesLoaded = true;
        }

        // Return up to batchSize records
        while (batch.size() < batchSize && currentLineIndex < lines.size()) {
          String line = lines.get(currentLineIndex++);
          JsonNode jsonNode = OBJECT_MAPPER.readTree("{\"data\":\"" + line + "\"}");
          batch.add(new ImportRow(currentRowNumber++, jsonNode));
          totalRecordsProcessed.incrementAndGet();
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading batch", e);
      }

      return batch;
    }

    // Add a tracking listener to monitor processing
    @Override
    public void addListener(ImportEventListener listener) {
      super.addListener(listener);
      // Also add internal tracking
      super.addListener(
          new ImportEventListener() {
            @Override
            public void onImportStarted(ImportStatus status) {
              // No action needed
            }

            @Override
            public void onImportCompleted(ImportStatus status) {
              // No action needed
            }

            @Override
            public void onAllImportsCompleted() {
              // No action needed
            }

            @Override
            public void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus) {
              // No action needed
            }

            @Override
            public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
              // No action needed
            }

            @Override
            public void onTaskComplete(ImportTaskResult taskResult) {
              // No action needed
            }
          });
    }
  }
}
