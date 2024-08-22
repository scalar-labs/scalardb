package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationUpdatedRecordRepository;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHandlerWorker extends BaseHandlerWorker<UpdatedRecord> {
  private static final Logger logger = LoggerFactory.getLogger(RecordHandlerWorker.class);

  private final Configuration conf;
  private final RecordHandler recordHandler;
  private final ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration extends BaseHandlerWorker.Configuration {
    private final int fetchSize;
    private final long skipRecentDataThresholdMillis;

    public Configuration(
        int replicationDbPartitionSize,
        int threadSize,
        int waitMillisPerPartition,
        int fetchSize,
        long skipRecentDataThresholdMillis) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
      this.skipRecentDataThresholdMillis = skipRecentDataThresholdMillis;
    }
  }

  static class ResultOfKeyHandling {
    final Long currentRecordVersion;
    final boolean remainingValueExists;
    final boolean nextConnectedValueExists;

    ResultOfKeyHandling(
        Long currentRecordVersion, boolean remainingValueExists, boolean nextConnectedValueExists) {
      this.currentRecordVersion = currentRecordVersion;
      this.remainingValueExists = remainingValueExists;
      this.nextConnectedValueExists = nextConnectedValueExists;
    }
  }

  public RecordHandlerWorker(
      Configuration conf,
      RecordHandler recordHandler,
      ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository,
      List<BlockingQueue<UpdatedRecord>> updatedRecordQueues,
      MetricsLogger metricsLogger) {
    super(conf, "record", metricsLogger);
    this.conf = conf;
    this.recordHandler = recordHandler;
    this.replicationUpdatedRecordRepository = replicationUpdatedRecordRepository;
    this.metricsLogger = metricsLogger;
  }

  @Override
  protected boolean handlePartition(int partitionId) throws ExecutionException {
    List<UpdatedRecord> scannedUpdatedRecords =
        metricsLogger.execFetchUpdatedRecords(
            () ->
                replicationUpdatedRecordRepository.scan(
                    partitionId, conf.fetchSize, conf.skipRecentDataThresholdMillis));

    boolean isImmediateRetryNeeded = false;

    for (UpdatedRecord updatedRecord : scannedUpdatedRecords) {
      if (recordHandler.handleUpdatedRecord(updatedRecord)) {
        isImmediateRetryNeeded = true;
      }
    }

    return isImmediateRetryNeeded;
  }
}
