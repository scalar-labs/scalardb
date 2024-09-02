package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.util.List;
import javax.annotation.concurrent.Immutable;

public class BulkTransactionScanWorker extends BaseScanWorker {
  private final Configuration conf;
  private final ReplicationBulkTransactionRepository replicationBulkTransactionRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final MetricsLogger metricsLogger;
  private final TransactionHandleWorker transactionHandleWorker;

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
      ReplicationTransactionRepository replicationTransactionRepository,
      TransactionHandleWorker transactionHandleWorker,
      MetricsLogger metricsLogger) {
    super(conf, "bulk-tx", metricsLogger);
    this.conf = conf;
    this.replicationBulkTransactionRepository = replicationBulkTransactionRepository;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.transactionHandleWorker = transactionHandleWorker;
    this.metricsLogger = metricsLogger;
  }

  private void moveTransaction(Transaction transaction) throws ExecutionException {
    metricsLogger.incrBlkTxnsScannedTxns();
    replicationTransactionRepository.add(transaction);
    transactionHandleWorker.enqueue(transaction);
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    List<BulkTransaction> scannedBulkTxns =
        metricsLogger.execScanBulkTransactions(
            () -> replicationBulkTransactionRepository.scan(partitionId, conf.fetchSize));
    for (BulkTransaction bulkTransaction : scannedBulkTxns) {
      for (Transaction transaction : bulkTransaction.transactions) {
        moveTransaction(transaction);
      }
      replicationBulkTransactionRepository.delete(bulkTransaction);
    }

    return scannedBulkTxns.size() >= conf.fetchSize;
  }
}
