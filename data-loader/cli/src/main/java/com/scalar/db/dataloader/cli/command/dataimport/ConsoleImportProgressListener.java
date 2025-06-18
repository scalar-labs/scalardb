package com.scalar.db.dataloader.cli.command.dataimport;

import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConsoleImportProgressListener implements ImportEventListener {

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final long startTime;
  private final Map<Integer, String> chunkLogs = new ConcurrentHashMap<>();
  private final Map<Integer, String> chunkFailureLogs = new ConcurrentHashMap<>();
  private final AtomicLong totalRecords = new AtomicLong();
  private final AtomicLong totalSuccess = new AtomicLong();
  private final AtomicLong totalFailures = new AtomicLong();
  private volatile boolean completed = false;

  public ConsoleImportProgressListener(Duration updateInterval) {
    startTime = System.currentTimeMillis();
    scheduler.scheduleAtFixedRate(
        this::render, 0, updateInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void onDataChunkStarted(ImportDataChunkStatus status) {
    chunkLogs.put(
        status.getDataChunkId(),
        String.format(
            "🔄 Chunk %d: Processing... %,d records so far",
            status.getDataChunkId(), totalRecords.get()));
  }

  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus status) {
    long elapsed = status.getEndTime().toEpochMilli() - status.getStartTime().toEpochMilli();
    totalRecords.addAndGet(status.getTotalRecords());
    totalSuccess.addAndGet(status.getSuccessCount());
    totalFailures.addAndGet(status.getFailureCount());
    if (status.getSuccessCount() > 0) {
      chunkLogs.put(
          status.getDataChunkId(),
          String.format(
              "✓ Chunk %d: %,d records imported (%.1fs), %d records imported successfully, import of %d records failed",
              status.getDataChunkId(),
              status.getTotalRecords(),
              elapsed / 1000.0,
              status.getSuccessCount(),
              status.getFailureCount()));
    }
  }

  @Override
  public void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus) {
    // Not used currently, but could be extended for detailed batch-level progress
  }

  @Override
  public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    if (!batchResult.isSuccess()) {
      chunkFailureLogs.put(
          batchResult.getDataChunkId(),
          String.format(
              "❌ Chunk id: %d, Transaction batch id: %d failed: %,d records could not be imported",
              batchResult.getDataChunkId(),
              batchResult.getTransactionBatchId(),
              batchResult.getRecords().size()));
    }
  }

  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    // No-op currently, could be extended to summarize task-level results
  }

  @Override
  public void onAllDataChunksCompleted() {
    completed = true;
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      scheduler.shutdownNow();
    }
    render(); // Final render after shutdown
  }

  private void render() {
    StringBuilder builder = new StringBuilder();
    long now = System.currentTimeMillis();
    long elapsed = now - startTime;
    double recPerSec = (totalRecords.get() * 1000.0) / (elapsed == 0 ? 1 : elapsed);

    builder.append(
        String.format(
            "\rImporting... %,d records | %.0f rec/s | %s\n",
            totalRecords.get(), recPerSec, formatElapsed(elapsed)));

    chunkLogs.values().stream().sorted().forEach(line -> builder.append(line).append("\n"));
    chunkFailureLogs.values().stream().sorted().forEach(line -> builder.append(line).append("\n"));

    if (completed) {
      builder.append(
          String.format(
              "\n✅ Import completed: %,d records succeeded, %,d failed\n",
              totalSuccess.get(), totalFailures.get()));
    }
    clearConsole();
    System.out.print(builder);
    System.out.flush();
  }

  private String formatElapsed(long elapsedMillis) {
    long seconds = (elapsedMillis / 1000) % 60;
    long minutes = (elapsedMillis / 1000) / 60;
    return String.format("%dm %02ds elapsed", minutes, seconds);
  }

  private void clearConsole() {
    System.out.print("\033[H\033[2J"); // ANSI escape to clear screen
    System.out.flush();
  }
}
