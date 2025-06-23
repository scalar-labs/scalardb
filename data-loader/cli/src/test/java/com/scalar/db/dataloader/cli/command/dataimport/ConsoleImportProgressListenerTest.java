package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.TextNode;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatusState;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;

class ConsoleImportProgressListenerTest {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private ConsoleImportProgressListener listener;

  @BeforeEach
  void setUp() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, true, "UTF-8"));
    listener = new ConsoleImportProgressListener(Duration.ofMillis(100));
  }

  @AfterEach
  void tearDown() {
    System.setOut(originalOut);
  }

  @Test
  void testOnDataChunkStartedAndCompleted_shouldTrackSuccessAndFailureCounts()
      throws InterruptedException, UnsupportedEncodingException {
    int chunkId = 1;
    Instant start = Instant.now();
    TimeUnit.MILLISECONDS.sleep(50);
    Instant end = Instant.now();

    ImportDataChunkStatus startStatus =
        ImportDataChunkStatus.builder()
            .dataChunkId(chunkId)
            .startTime(start)
            .totalRecords(100)
            .successCount(0)
            .failureCount(0)
            .batchCount(0)
            .endTime(null)
            .totalDurationInMilliSeconds(0)
            .status(ImportDataChunkStatusState.IN_PROGRESS)
            .build();

    listener.onDataChunkStarted(startStatus);

    ImportDataChunkStatus completedStatus =
        ImportDataChunkStatus.builder()
            .dataChunkId(chunkId)
            .startTime(start)
            .endTime(end)
            .totalRecords(100)
            .successCount(90)
            .failureCount(10)
            .batchCount(1)
            .totalDurationInMilliSeconds((int) (end.toEpochMilli() - start.toEpochMilli()))
            .status(ImportDataChunkStatusState.COMPLETE)
            .build();

    listener.onDataChunkCompleted(completedStatus);

    TimeUnit.MILLISECONDS.sleep(200); // Allow render
    listener.onAllDataChunksCompleted();

    String output = outContent.toString("UTF-8");

    assertTrue(output.contains("✓ Chunk 1"), "Expected chunk log line");
    assertTrue(output.contains("90 records imported successfully"), "Expected success count");
    assertTrue(output.contains("10 records failed"), "Expected failure count");
    assertTrue(
        output.contains("✅ Import completed: 90 records succeeded, 10 failed"),
        "Expected final summary");
  }

  @Test
  void testOnTransactionBatchFailed_shouldAccumulateFailuresInChunkFailureLogs()
      throws InterruptedException, UnsupportedEncodingException {
    // Arrange: Build dummy failed batch
    ImportTaskResult task =
        ImportTaskResult.builder()
            .rowNumber(1)
            .targets(Collections.emptyList())
            .rawRecord(TextNode.valueOf("{\"id\":1}"))
            .dataChunkId(7)
            .build();

    ImportTransactionBatchResult failedBatch =
        ImportTransactionBatchResult.builder()
            .transactionBatchId(2)
            .dataChunkId(7)
            .transactionId("txn-123")
            .records(Collections.nCopies(5, task))
            .errors(Arrays.asList("error1", "error2"))
            .success(false)
            .build();

    // Act
    listener.onTransactionBatchCompleted(failedBatch);

    // Trigger final flush
    listener.onAllDataChunksCompleted();

    // Give time for render() to complete after scheduler shutdown
    TimeUnit.MILLISECONDS.sleep(500);

    // Assert
    String output = outContent.toString("UTF-8");
    System.out.println("OUTPUT:\n" + output); // Useful for debug
    assertTrue(
        output.contains("❌ Chunk id: 7, Transaction batch id: 2 failed"),
        "Expected failure message");
    //        assertTrue(output.contains("5 records failed to be imported"), "Expected failed record
    // count in batch");
    //        assertTrue(output.contains("✅ Import completed: 0 records succeeded, 5 failed"),
    // "Expected final summary with failure count");
  }

  @Test
  void testOnAllDataChunksCompleted_shouldShutdownAndPrintFinalSummary()
      throws UnsupportedEncodingException {
    listener.onAllDataChunksCompleted();
    String output = outContent.toString("UTF-8");
    assertTrue(output.contains("✅ Import completed"), "Should print completion summary");
  }

  @Test
  void testOnTransactionBatchStarted_shouldRunWithoutException() {
    ImportTransactionBatchStatus status =
        ImportTransactionBatchStatus.builder()
            .dataChunkId(3)
            .transactionBatchId(1)
            .transactionId("txn-1")
            .records(Collections.emptyList())
            .errors(Collections.emptyList())
            .success(true)
            .build();

    listener.onTransactionBatchStarted(status); // Should not throw
  }

  @Test
  void testOnTaskComplete_shouldRunWithoutException() {
    ImportTaskResult task =
        ImportTaskResult.builder()
            .rowNumber(42)
            .targets(Collections.emptyList())
            .rawRecord(TextNode.valueOf("{}"))
            .dataChunkId(99)
            .build();

    listener.onTaskComplete(task); // Should not throw
  }
}
