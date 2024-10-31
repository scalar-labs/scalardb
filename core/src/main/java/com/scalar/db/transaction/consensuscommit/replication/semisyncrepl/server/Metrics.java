package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

class Metrics {
  public final AtomicInteger blkTxnScannedTxns = new AtomicInteger();
  public final AtomicInteger txnScannedTxns = new AtomicInteger();
  public final AtomicInteger txnUncommittedTxns = new AtomicInteger();
  public final AtomicInteger txnAbortedTxns = new AtomicInteger();
  public final AtomicInteger txnCommittedTxns = new AtomicInteger();
  public final AtomicLong txnOpDurationMillisToScanTxns = new AtomicLong();
  public final AtomicInteger txnOpCountToScanTxns = new AtomicInteger();
  public final AtomicLong blkTxnOpDurationMillisToScanBlkTxns = new AtomicLong();
  public final AtomicInteger blkTxnOpCountToScanBlkTxns = new AtomicInteger();
  public final AtomicLong txnOpDurationMillisToAppendValueToRecord = new AtomicLong();
  public final AtomicInteger txnOpCountToAppendValueToRecord = new AtomicInteger();
  public final AtomicInteger recordOpCountToGetRecord = new AtomicInteger();
  public final AtomicLong recordOpDurationMillisToGetRecord = new AtomicLong();
  public final AtomicLong recordOpDurationMillisToUpdateRecord = new AtomicLong();
  public final AtomicInteger recordOpCountToUpdateRecord = new AtomicInteger();
  public final AtomicInteger coordSteteOpCountToGet = new AtomicInteger();
  public final AtomicLong coordStateOpDurationMillisToGet = new AtomicLong();
  public final AtomicInteger recordHandleTxns = new AtomicInteger();
  public final AtomicInteger recordRetryTxns = new AtomicInteger();
  public final AtomicInteger exceptions = new AtomicInteger();

  @Nullable private final TransactionHandleWorker transactionHandleWorker;

  public Metrics(@Nullable TransactionHandleWorker transactionHandleWorker) {
    this.transactionHandleWorker = transactionHandleWorker;
  }

  private double meanDuration(int count, long durationInMillis) {
    if (count == 0) {
      return 0.0;
    } else {
      return (double) durationInMillis / count;
    }
  }

  public String toJson() {
    String transactionHandleWorkerJson;
    {
      TransactionHandleWorker worker = transactionHandleWorker;
      if (worker == null) {
        transactionHandleWorkerJson = "{}";
      } else {
        transactionHandleWorkerJson = worker.toJson();
      }
    }

    return String.format(
        "{\n"
            + "  \"CurrentTimestampMs\":%d,\n"
            + "  \"Common\":{\"Exceptions\":%d},\n"
            + "  \"BulkTxn\":{\n"
            + "    \"ScannedTxns\":%d,\n"
            + "    \"Ops\":{\n"
            + "      \"ScanBlkTxns\":{\"Count\":%d, \"DurationMs\":%f}\n"
            + "    }\n"
            + "  },\n"
            + "  \"CoordState\":{\n"
            + "    \"Ops\":{\n"
            + "      \"Get\":{\"Count\":%d, \"DurationMs\":%f}\n"
            + "    }\n"
            + "  },\n"
            + "  \"Txn\":{\n"
            + "    \"ScannedTxns\":%d,\n"
            + "    \"TxnState\":{\n"
            + "      \"CommittedTxns\":%d,\n"
            + "      \"UncommittedTxns\":%d,\n"
            + "      \"AbortedTxns\":%d\n"
            + "    },\n"
            + "    \"Ops\":{\n"
            + "      \"ScanTxns\":{\"Count\":%d, \"DurationMs\":%f},\n"
            + "      \"AppendValueToRecord\":{\"Count\":%d, \"DurationMs\":%f}\n"
            + "    }\n"
            + "  },\n"
            + "  \"Record\":{\n"
            + "    \"HandleTxns\":%d,\n"
            + "    \"RetryTxns\":%d,\n"
            + "    \"Ops\":{\n"
            + "      \"GetRecord\":{\"Count\":%d, \"DurationMs\":%f},\n"
            + "      \"UpdateRecord\":{\"Count\":%d, \"DurationMs\":%f}\n"
            + "    }\n"
            + "  },\n"
            + "  \"TxnHandleWorker\":%s\n"
            + "}",
        System.currentTimeMillis(),
        exceptions.get(),
        blkTxnScannedTxns.get(),
        blkTxnOpCountToScanBlkTxns.get(),
        meanDuration(blkTxnOpCountToScanBlkTxns.get(), blkTxnOpDurationMillisToScanBlkTxns.get()),
        coordSteteOpCountToGet.get(),
        meanDuration(coordSteteOpCountToGet.get(), coordStateOpDurationMillisToGet.get()),
        txnScannedTxns.get(),
        txnCommittedTxns.get(),
        txnUncommittedTxns.get(),
        txnAbortedTxns.get(),
        txnOpCountToScanTxns.get(),
        meanDuration(txnOpCountToScanTxns.get(), txnOpDurationMillisToScanTxns.get()),
        txnOpCountToAppendValueToRecord.get(),
        meanDuration(
            txnOpCountToAppendValueToRecord.get(), txnOpDurationMillisToAppendValueToRecord.get()),
        recordHandleTxns.get(),
        recordRetryTxns.get(),
        recordOpCountToGetRecord.get(),
        meanDuration(recordOpCountToGetRecord.get(), recordOpDurationMillisToGetRecord.get()),
        recordOpCountToUpdateRecord.get(),
        meanDuration(recordOpCountToUpdateRecord.get(), recordOpDurationMillisToUpdateRecord.get()),
        transactionHandleWorkerJson);
  }
}
