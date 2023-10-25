package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class MetricsLogger {
  private final boolean isEnabled;
  private final Map<Instant, Metrics> metricsMap = new ConcurrentHashMap<>();
  private final AtomicReference<Instant> keyHolder = new AtomicReference<>();
  private final Queue<Key> recordWriterQueue;

  public MetricsLogger(Queue<Key> recordWriterQueue) {
    String metricsEnabled = System.getenv("LOG_APPLIER_METRICS_ENABLED");
    this.isEnabled = metricsEnabled != null && metricsEnabled.equalsIgnoreCase("true");
    this.recordWriterQueue = recordWriterQueue;
  }

  private Instant currentTimestampRoundedInSeconds() {
    return Instant.ofEpochSecond(System.currentTimeMillis() / 1000);
  }

  private void withPrintAndCleanup(Consumer<Metrics> consumer) {
    Instant currentKey = currentTimestampRoundedInSeconds();
    Instant oldKey = keyHolder.getAndSet(currentKey);
    Metrics metrics = metricsMap.computeIfAbsent(currentKey, k -> new Metrics(recordWriterQueue));
    consumer.accept(metrics);
    if (oldKey == null) {
      return;
    }
    if (!oldKey.equals(currentKey)) {
      // logger.info("[{}] {}", currentKey, metricsMap.get(oldKey));
      System.out.printf("[%s] %s\n", currentKey, metricsMap.get(oldKey));
    }
  }

  public void incrementScannedTransactions() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.scannedTransactions.incrementAndGet());
  }

  public void incrementHandledCommittedTransactions() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.handledCommittedTransactions.incrementAndGet());
  }

  public void incrementUncommittedTransactions() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.uncommittedTransactions.incrementAndGet());
  }

  public void incrementExceptionCount() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.exceptionCountInDistributor.incrementAndGet());
  }

  public static class ResultWithDuration<T> {
    public T result;
    public long durationInMillis;

    public ResultWithDuration(T result, long durationInMillis) {
      this.result = result;
      this.durationInMillis = durationInMillis;
    }
  }

  @FunctionalInterface
  public interface Task<T> {
    T exec() throws Throwable;
  }

  public <T> ResultWithDuration<T> captureDuration(Task<T> task) {
    long start = System.currentTimeMillis();
    T result;
    try {
      result = task.exec();
    } catch (Throwable e) {
      // FIXME
      throw new RuntimeException(e);
    }
    long end = System.currentTimeMillis();
    return new ResultWithDuration<>(result, end - start);
  }

  public List<Transaction> execFetchTransactions(Task<List<Transaction>> task) {
    ResultWithDuration<List<Transaction>> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.totalCountToFetchTransaction.incrementAndGet();
          metrics.totalDurationInMillisToFetchTransaction.addAndGet(
              resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public List<BulkTransaction> execFetchBulkTransactions(Task<List<BulkTransaction>> task) {
    ResultWithDuration<List<BulkTransaction>> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.totalCountToFetchBulkTransaction.incrementAndGet();
          metrics.totalDurationInMillisToFetchBulkTransaction.addAndGet(
              resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public void execAppendValueToRecord(Task<Void> task) {
    ResultWithDuration<Void> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.totalCountToAppendValueToRecord.incrementAndGet();
          metrics.totalDurationInMillisToAppendValueToRecord.addAndGet(
              resultWithDuration.durationInMillis);
        });
  }

  public Optional<Record> execGetRecord(Task<Optional<Record>> task) {
    ResultWithDuration<Optional<Record>> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.totalCountToGetRecord.incrementAndGet();
          metrics.totalDurationInMillisToGetRecord.addAndGet(resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public void execSetPrepTxIdInRecord(Task<Void> task) {
    ResultWithDuration<Void> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.totalCountToSetPrepTxIdInRecord.incrementAndGet();
          metrics.totalDurationInMillisToSetPrepTxIdInRecord.addAndGet(
              resultWithDuration.durationInMillis);
        });
  }

  public void execUpdateRecord(Task<Void> task) {
    ResultWithDuration<Void> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.totalCountToUpdateRecord.incrementAndGet();
          metrics.totalDurationInMillisToUpdateRecord.addAndGet(
              resultWithDuration.durationInMillis);
        });
  }
}
