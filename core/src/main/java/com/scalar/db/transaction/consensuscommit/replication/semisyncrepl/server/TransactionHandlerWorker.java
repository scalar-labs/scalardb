package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.util.List;
import javax.annotation.concurrent.Immutable;

public class TransactionHandlerWorker extends BaseHandlerWorker<UpdatedRecord> {
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final TransactionHandlerWorker.Configuration conf;
  private final MetricsLogger metricsLogger;
  private final TransactionHandler transactionHandler;

  @Immutable
  public static class Configuration extends BaseHandlerWorker.Configuration {
    private final int fetchSize;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition, int fetchSize) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
    }
  }

  public TransactionHandlerWorker(
      Configuration conf,
      TransactionHandler transactionHandler,
      ReplicationTransactionRepository replicationTransactionRepository,
      MetricsLogger metricsLogger) {
    super(conf, "tx", metricsLogger);
    this.conf = conf;
    this.transactionHandler = transactionHandler;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.metricsLogger = metricsLogger;
  }

  @Override
  protected boolean handlePartition(int partitionId) throws ExecutionException {
    List<Transaction> scannedTxns =
        metricsLogger.execFetchTransactions(
            () -> replicationTransactionRepository.scan(partitionId, conf.fetchSize));
    int finishedTransactions = 0;
    for (Transaction transaction : scannedTxns) {
      if (transactionHandler.handleTransaction(transaction)) {
        finishedTransactions++;
      }
    }

    return finishedTransactions >= conf.fetchSize;
  }
}
