package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.util.List;
import javax.annotation.concurrent.Immutable;

public class BulkTransactionHandlerWorker extends BaseHandlerWorker<Void> {
  private final Configuration conf;
  private final ReplicationBulkTransactionRepository replicationBulkTransactionRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration extends BaseHandlerWorker.Configuration {
    private final int fetchSize;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition, int fetchSize) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
    }
  }

  public BulkTransactionHandlerWorker(
      Configuration conf,
      ReplicationBulkTransactionRepository replicationBulkTransactionRepository,
      ReplicationTransactionRepository replicationTransactionRepository,
      MetricsLogger metricsLogger) {
    super(conf, "bulk-tx", metricsLogger, null, null);
    this.conf = conf;
    this.replicationBulkTransactionRepository = replicationBulkTransactionRepository;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.metricsLogger = metricsLogger;
  }

  private void moveTransaction(Transaction transaction) throws ExecutionException {
    metricsLogger.incrementScannedTransactions();
    replicationTransactionRepository.add(transaction);
    metricsLogger.incrementHandledCommittedTransactions();
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    List<BulkTransaction> scannedBulkTxns =
        metricsLogger.execFetchBulkTransactions(
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
