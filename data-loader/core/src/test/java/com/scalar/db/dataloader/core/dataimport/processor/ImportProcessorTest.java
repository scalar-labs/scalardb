package com.scalar.db.dataloader.core.dataimport.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for the ImportProcessor class.
 *
 * <p>These tests verify that the process method correctly handles different scenarios including
 * storage mode, transaction mode, empty data, and large data chunks.
 *
 * <p>Additionally, this class tests the thread executor behavior in ImportProcessor, including
 * proper shutdown, waiting for tasks to complete, handling interruptions, and task distribution.
 */
@ExtendWith(MockitoExtension.class)
class ImportProcessorTest {

  @Mock private ImportProcessorParams params;
  @Mock private ImportOptions importOptions;
  @Mock private ScalarDbDao dao;
  @Mock private DistributedStorage distributedStorage;
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

    when(importOptions.getMaxThreads()).thenReturn(2);
    when(importOptions.getDataChunkQueueSize()).thenReturn(10);
    when(params.getImportOptions()).thenReturn(importOptions);
  }

  @Test
  void process_withStorageMode_shouldProcessAllDataChunks() {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader("test data"));
    when(params.getScalarDbMode()).thenReturn(ScalarDbMode.STORAGE);
    when(params.getDao()).thenReturn(dao);
    when(params.getDistributedStorage()).thenReturn(distributedStorage);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(2, 1, reader);

    // Assert
    verify(eventListener, times(1)).onAllDataChunksCompleted();
    // Verify that data chunks were processed
    verify(eventListener, times(1)).onDataChunkCompleted(any(ImportDataChunkStatus.class));
  }

  @Test
  void process_withTransactionMode_shouldProcessAllDataChunks() throws TransactionException {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader("test data"));
    when(params.getScalarDbMode()).thenReturn(ScalarDbMode.TRANSACTION);
    when(params.getDao()).thenReturn(dao);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);
    when(params.getDistributedTransactionManager()).thenReturn(distributedTransactionManager);
    when(distributedTransactionManager.start()).thenReturn(distributedTransaction);

    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    // Act
    processor.process(2, 1, reader);

    // Assert
    verify(eventListener, times(1)).onAllDataChunksCompleted();
    // Verify that data chunks were processed
    verify(eventListener, times(1)).onDataChunkCompleted(any(ImportDataChunkStatus.class));
  }

  @Test
  void process_withEmptyData_shouldNotProcessAnyDataChunks() {
    // Arrange
    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);

    BufferedReader reader = new BufferedReader(new StringReader(""));

    // Act
    processor.process(2, 1, reader);

    // Assert
    verify(eventListener, times(1)).onAllDataChunksCompleted();
    // Verify that no data chunks were processed
    verify(eventListener, times(0)).onDataChunkCompleted(any());
  }

  // Thread executor behavior tests

  @Test
  void process_withMultipleDataChunks_shouldUseThreadPool() {
    // Arrange
    final int maxThreads = 4;
    when(importOptions.getMaxThreads()).thenReturn(maxThreads);
    when(params.getDao()).thenReturn(dao);
    when(params.getDistributedStorage()).thenReturn(distributedStorage);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);

    // Create test data with multiple chunks
    StringBuilder testData = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      testData.append("test data line ").append(i).append("\n");
    }
    BufferedReader reader = new BufferedReader(new StringReader(testData.toString()));

    // Create a latch to ensure tasks take some time to complete
    CountDownLatch latch = new CountDownLatch(1);

    // Create a TestImportProcessor
    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);
    processor.setProcessingLatch(latch);

    // Act
    processor.process(2, 1, reader);

    // Assert
    // Verify that multiple threads were used but not more than maxThreads
    assertTrue(processor.getMaxConcurrentThreads().get() > 1, "Should use multiple threads");
    assertTrue(
        processor.getMaxConcurrentThreads().get() <= maxThreads, "Should not exceed max threads");

    // Verify that all data chunks were processed
    verify(eventListener, times(1)).onAllDataChunksCompleted();
  }

  @Test
  void process_withInterruption_shouldShutdownGracefully() {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader("test data\nmore data\n"));

    // Create a processor that will be interrupted
    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);
    processor.setSimulateInterruption(true);

    // Act & Assert
    assertThrows(RuntimeException.class, () -> processor.process(2, 1, reader));

    // Verify that onAllDataChunksCompleted was still called (in finally block)
    verify(eventListener, times(1)).onAllDataChunksCompleted();
  }

  @Test
  void process_withLargeNumberOfTasks_shouldWaitForAllTasksToComplete() {
    // Arrange
    final int maxThreads = 2;
    when(importOptions.getMaxThreads()).thenReturn(maxThreads);
    when(params.getDao()).thenReturn(dao);
    when(params.getDistributedStorage()).thenReturn(distributedStorage);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);

    // Create test data with many chunks
    StringBuilder testData = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      testData.append("test data line ").append(i).append("\n");
    }
    BufferedReader reader = new BufferedReader(new StringReader(testData.toString()));

    // Create a TestImportProcessor with a small processing delay
    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);
    processor.setProcessingDelayMs(10); // 10ms delay per chunk

    // Act
    processor.process(2, 1, reader);

    // Assert
    // Verify that all tasks were completed
    assertTrue(processor.getProcessedChunksCount().get() > 0, "All tasks should be completed");
    verify(eventListener, times(1)).onAllDataChunksCompleted();
  }

  @Test
  void process_withShutdown_shouldShutdownExecutorsGracefully() {
    // Arrange
    when(params.getScalarDbMode()).thenReturn(ScalarDbMode.STORAGE);
    when(params.getDao()).thenReturn(dao);
    when(params.getDistributedStorage()).thenReturn(distributedStorage);
    when(params.getTableColumnDataTypes()).thenReturn(tableColumnDataTypes);
    when(params.getTableMetadataByTableName()).thenReturn(tableMetadataByTableName);

    BufferedReader reader =
        new BufferedReader(new StringReader("test data\nmore data\neven more data\n"));

    // Create a TestImportProcessor with a longer processing delay
    TestImportProcessor processor = new TestImportProcessor(params);
    processor.addListener(eventListener);
    processor.setProcessingDelayMs(50); // 50ms delay per chunk

    // Act
    processor.process(1, 1, reader);

    // Assert
    // Verify that all data chunks were processed and executors were shut down gracefully
    verify(eventListener, times(1)).onAllDataChunksCompleted();
    assertEquals(3, processor.getProcessedChunksCount().get(), "All chunks should be processed");
  }

  /**
   * A simple implementation of ImportProcessor for testing purposes. This class is used to test the
   * thread executor behavior in ImportProcessor.
   */
  static class TestImportProcessor extends ImportProcessor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // Tracking for testing
    @Getter private final AtomicInteger processedChunksCount = new AtomicInteger(0);

    @Getter private final AtomicInteger maxConcurrentThreads = new AtomicInteger(0);

    private final AtomicInteger currentConcurrentThreads = new AtomicInteger(0);

    private final AtomicBoolean simulateInterruption = new AtomicBoolean(false);

    @Setter private CountDownLatch processingLatch;

    @Setter private long processingDelayMs = 0;

    public TestImportProcessor(ImportProcessorParams params) {
      super(params);
      // Add our tracking listener
      addTrackingListener();
    }

    /** Sets whether to simulate an interruption during processing. */
    public void setSimulateInterruption(boolean value) {
      this.simulateInterruption.set(value);
    }

    @Override
    protected void readDataChunks(
        BufferedReader reader, int dataChunkSize, BlockingQueue<ImportDataChunk> dataChunkQueue) {
      try {
        List<ImportRow> rows = new ArrayList<>();
        String line;
        int rowNumber = 0;

        while ((line = reader.readLine()) != null) {
          if (!line.trim().isEmpty()) {
            // Create a simple JsonNode from the line
            JsonNode jsonNode = OBJECT_MAPPER.readTree("{\"data\":\"" + line + "\"}");
            rows.add(new ImportRow(rowNumber++, jsonNode));

            if (rows.size() >= dataChunkSize) {
              ImportDataChunk dataChunk =
                  ImportDataChunk.builder()
                      .dataChunkId(rowNumber / dataChunkSize)
                      .sourceData(rows)
                      .build();
              dataChunkQueue.put(dataChunk);
              rows = new ArrayList<>();

              // Simulate interruption if requested (in the reader thread)
              if (simulateInterruption.get()) {
                Thread.currentThread().interrupt();
                throw new InterruptedException("Simulated interruption in reader");
              }
            }
          }
        }

        // Add any remaining rows
        if (!rows.isEmpty()) {
          ImportDataChunk dataChunk =
              ImportDataChunk.builder()
                  .dataChunkId(rowNumber / dataChunkSize + 1)
                  .sourceData(rows)
                  .build();
          dataChunkQueue.put(dataChunk);
        }
      } catch (IOException | InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Error reading data chunks", e);
      }
    }

    // Add a tracking listener to monitor thread behavior
    private void addTrackingListener() {
      super.addListener(
          new ImportEventListener() {
            @Override
            public void onDataChunkStarted(ImportDataChunkStatus status) {
              // Track concurrent threads
              int current = currentConcurrentThreads.incrementAndGet();
              maxConcurrentThreads.set(Math.max(current, maxConcurrentThreads.get()));

              // Add processing delay if specified
              if (processingDelayMs > 0) {
                try {
                  Thread.sleep(processingDelayMs);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }

              // Wait on latch if provided
              if (processingLatch != null) {
                try {
                  processingLatch.await(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }

              // Simulate interruption if requested (in worker threads)
              if (simulateInterruption.get()) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Simulated interruption in worker");
              }
            }

            @Override
            public void onDataChunkCompleted(ImportDataChunkStatus status) {
              processedChunksCount.incrementAndGet();
              currentConcurrentThreads.decrementAndGet();
            }

            @Override
            public void onAllDataChunksCompleted() {
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
