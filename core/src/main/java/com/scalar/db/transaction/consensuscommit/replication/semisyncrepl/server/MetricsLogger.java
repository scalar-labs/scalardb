package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository.ScanResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class MetricsLogger {
  private final boolean isEnabled;
  private final Map<Instant, Metrics> metricsMap = new ConcurrentHashMap<>();
  private final AtomicReference<Instant> keyHolder = new AtomicReference<>();
  private final AtomicReference<TransactionHandleWorker> transactionHandleWorker =
      new AtomicReference<>();

  public MetricsLogger() {
    String metricsEnabled = System.getenv("LOG_APPLIER_METRICS_ENABLED");
    this.isEnabled = metricsEnabled != null && metricsEnabled.equalsIgnoreCase("true");
  }

  public void setTransactionHandleWorker(TransactionHandleWorker transactionHandleWorker) {
    this.transactionHandleWorker.set(transactionHandleWorker);
  }

  private Instant currentTimestampRoundedInSeconds() {
    return Instant.ofEpochSecond(System.currentTimeMillis() / 1000);
  }

  private void withPrintAndCleanup(Consumer<Metrics> consumer) {
    Instant currentKey = currentTimestampRoundedInSeconds();
    Instant oldKey = keyHolder.getAndSet(currentKey);
    Metrics metrics =
        metricsMap.computeIfAbsent(currentKey, k -> new Metrics(transactionHandleWorker.get()));
    consumer.accept(metrics);
    if (oldKey == null) {
      return;
    }
    if (!oldKey.equals(currentKey)) {
      System.out.printf("[%s] Metrics:\n%s\n\n", currentKey, metricsMap.get(oldKey).toJson());
    }
  }

  public void incrBlkTxnsScannedTxns() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.blkTxnScannedTxns.incrementAndGet());
  }

  public void incrTxnsScannedTxns() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.txnScannedTxns.incrementAndGet());
  }

  public void incrCommittedTxns() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.txnCommittedTxns.incrementAndGet());
  }

  public void incrementUncommittedTransactions() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.txnUncommittedTxns.incrementAndGet());
  }

  public void incrementAbortedTransactions() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.txnAbortedTxns.incrementAndGet());
  }

  public void incrementHandleTransaction() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.recordHandleTxns.incrementAndGet());
  }

  public void incrementRetryTransaction() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.recordRetryTxns.incrementAndGet());
  }

  public void incrementExceptionCount() {
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(metrics -> metrics.exceptions.incrementAndGet());
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

  public ScanResult execScanTransactions(Task<ScanResult> task) {
    ResultWithDuration<ScanResult> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.txnOpCountToScanTxns.incrementAndGet();
          metrics.txnOpDurationMillisToScanTxns.addAndGet(resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public List<BulkTransaction> execScanBulkTransactions(Task<List<BulkTransaction>> task) {
    ResultWithDuration<List<BulkTransaction>> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.blkTxnOpCountToScanBlkTxns.incrementAndGet();
          metrics.blkTxnOpDurationMillisToScanBlkTxns.addAndGet(
              resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public Record execAppendValueToRecord(Task<Record> task) {
    ResultWithDuration<Record> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.txnOpCountToAppendValueToRecord.incrementAndGet();
          metrics.txnOpDurationMillisToAppendValueToRecord.addAndGet(
              resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public Optional<Record> execGetRecord(Task<Optional<Record>> task) {
    ResultWithDuration<Optional<Record>> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.recordOpCountToGetRecord.incrementAndGet();
          metrics.recordOpDurationMillisToGetRecord.addAndGet(resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public Optional<Coordinator.State> execGetCoordinatorState(
      Task<Optional<Coordinator.State>> task) {
    ResultWithDuration<Optional<Coordinator.State>> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return resultWithDuration.result;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.coordSteteOpCountToGet.incrementAndGet();
          metrics.coordStateOpDurationMillisToGet.addAndGet(resultWithDuration.durationInMillis);
        });
    return resultWithDuration.result;
  }

  public void execUpdateRecord(Task<Void> task) {
    ResultWithDuration<Void> resultWithDuration = captureDuration(task);
    if (!isEnabled) {
      return;
    }
    withPrintAndCleanup(
        metrics -> {
          metrics.recordOpCountToUpdateRecord.incrementAndGet();
          metrics.recordOpDurationMillisToUpdateRecord.addAndGet(
              resultWithDuration.durationInMillis);
        });
  }
}
