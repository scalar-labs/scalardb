package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository.ScanResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkTransactionScanWorker extends BaseScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(BulkTransactionScanWorker.class);
  private final Configuration conf;
  private final ReplicationBulkTransactionRepository replicationBulkTransactionRepository;
  private final TransactionHandleWorker transactionHandleWorker;
  private final MetricsLogger metricsLogger;
  private final Map<Integer, Long> lastScannedTimestampMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> lastBulkTransactionTimestampMap = new ConcurrentHashMap<>();
  private final Set<String> ongoingBulkTransactionIds = new ConcurrentSkipListSet<>();

  @Immutable
  public static class Configuration extends BaseScanWorker.Configuration {
    private final int fetchSize;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition, int fetchSize) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
    }
  }

  public BulkTransactionScanWorker(
      Configuration conf,
      ReplicationBulkTransactionRepository replicationBulkTransactionRepository,
      TransactionHandleWorker transactionHandleWorker,
      MetricsLogger metricsLogger) {
    super(conf, "bulk-tx", metricsLogger);
    this.conf = conf;
    this.replicationBulkTransactionRepository = replicationBulkTransactionRepository;
    this.transactionHandleWorker = transactionHandleWorker;
    this.metricsLogger = metricsLogger;
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    @Nullable Long scanStartTsMillis = lastScannedTimestampMap.get(partitionId);
    @Nullable Long previousLastBulkTxnTsMillis = lastBulkTransactionTimestampMap.get(partitionId);

    ScanResult scanResult =
        metricsLogger.execScanBulkTransactions(
            () ->
                replicationBulkTransactionRepository.scan(
                    partitionId, conf.fetchSize, scanStartTsMillis));
    List<BulkTransaction> scannedBulkTxns = scanResult.bulkTransactions;
    metricsLogger.incrBlkTxnsScannedTxns(scannedBulkTxns.size());
    if (scannedBulkTxns.isEmpty()) {
      assert scanResult.nextScanTimestampMillis == null;
      lastScannedTimestampMap.remove(partitionId);
      lastBulkTransactionTimestampMap.remove(partitionId);
      return false;
    }

    Long lastBulkTxnTsMillis = null;
    for (BulkTransaction bulkTransaction : scannedBulkTxns) {
      if (ongoingBulkTransactionIds.contains(bulkTransaction.uniqueId)) {
        logger.info(
            "This BulkTransaction is still ongoing. Skipping it... Unique ID: {}, CreatedAt: {}\n",
            bulkTransaction.uniqueId,
            bulkTransaction.createdAt);
        continue;
      }

      long bulkTxnTsMillis = bulkTransaction.createdAt.toEpochMilli();
      if (previousLastBulkTxnTsMillis != null && bulkTxnTsMillis < previousLastBulkTxnTsMillis) {
        logger.warn(
            "Fetched older BulkTransaction than previously handled ones. Previous Last BulkTxnTs: {}, Unique ID: {}, TxnTs: {}.",
            Instant.ofEpochMilli(previousLastBulkTxnTsMillis),
            bulkTransaction.uniqueId,
            bulkTransaction.createdAt);
      }

      if (lastBulkTxnTsMillis == null || lastBulkTxnTsMillis < bulkTxnTsMillis) {
        lastBulkTxnTsMillis = bulkTxnTsMillis;
      }

      Set<Transaction> remainingTransactions =
          new ConcurrentSkipListSet<>(bulkTransaction.transactions);

      for (Transaction transaction : bulkTransaction.transactions) {
        transactionHandleWorker.enqueue(
            transaction,
            () -> {
              if (!remainingTransactions.remove(transaction)) {
                logger.warn(
                    "The Transaction {} wasn't contained in {}. Remaining transactions: {}",
                    transaction,
                    bulkTransaction,
                    remainingTransactions);
              }

              if (remainingTransactions.isEmpty()) {
                try {
                  replicationBulkTransactionRepository.delete(bulkTransaction);
                  if (!ongoingBulkTransactionIds.remove(bulkTransaction.uniqueId)) {
                    logger.warn(
                        "BulkTransaction {} doesn't exist in `ongoingBulkTransactionIds`",
                        bulkTransaction);
                  }
                } catch (ExecutionException e) {
                  // TODO
                  throw new RuntimeException(e);
                }
              }
            });
        ongoingBulkTransactionIds.add(bulkTransaction.uniqueId);
      }
    }

    if (scannedBulkTxns.size() < conf.fetchSize) {
      // It's likely no more record is stored.
      lastScannedTimestampMap.remove(partitionId);
      lastBulkTransactionTimestampMap.remove(partitionId);
    } else {
      assert scanResult.nextScanTimestampMillis != null;
      lastScannedTimestampMap.put(partitionId, scanResult.nextScanTimestampMillis);
      // FIXME: Just avoid NPE
      if (lastBulkTxnTsMillis != null) {
        lastBulkTransactionTimestampMap.put(partitionId, lastBulkTxnTsMillis);
      }
    }

    return scannedBulkTxns.size() >= conf.fetchSize;
  }
}
