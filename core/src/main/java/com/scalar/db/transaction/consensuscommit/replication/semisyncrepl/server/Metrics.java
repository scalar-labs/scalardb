package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Metrics {
  public final AtomicInteger scannedTransactions = new AtomicInteger();
  public final AtomicInteger uncommittedTransactions = new AtomicInteger();
  public final AtomicInteger abortedTransactions = new AtomicInteger();
  public final AtomicInteger handledCommittedTransactions = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToFetchTransaction = new AtomicLong();
  public final AtomicInteger totalCountToFetchTransaction = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToFetchBulkTransaction = new AtomicLong();
  public final AtomicInteger totalCountToFetchBulkTransaction = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToFetchUpdatedRecords = new AtomicLong();
  public final AtomicInteger totalCountToFetchUpdatedRecords = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToAppendValueToRecord = new AtomicLong();
  public final AtomicInteger totalCountToAppendValueToRecord = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToSetPrepTxIdInRecord = new AtomicLong();
  public final AtomicInteger totalCountToGetRecord = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToGetRecord = new AtomicLong();
  public final AtomicInteger totalCountToSetPrepTxIdInRecord = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToUpdateRecord = new AtomicLong();
  public final AtomicInteger totalCountToUpdateRecord = new AtomicInteger();
  public final AtomicInteger totalCountToDequeueFromTransactionQueue = new AtomicInteger();
  public final AtomicInteger totalCountToReEnqueueFromTransactionQueue = new AtomicInteger();
  public final AtomicInteger totalCountToDequeueFromUpdateRecordQueue = new AtomicInteger();
  public final AtomicInteger totalCountToReEnqueueFromUpdateRecordQueue = new AtomicInteger();
  public final AtomicInteger exceptionCountInDistributor = new AtomicInteger();

  private final InMemoryQueue<Transaction> transactionQueue;
  private final InMemoryQueue<Key> updatedRecordQueue;

  public Metrics(
      InMemoryQueue<Transaction> transactionQueue, InMemoryQueue<Key> updatedRecordQueue) {
    this.transactionQueue = transactionQueue;
    this.updatedRecordQueue = updatedRecordQueue;
  }

  private void addDuration(
      ToStringHelper stringHelper,
      String keyForCount,
      String keyForDuration,
      int count,
      long durationInMillis) {
    stringHelper.add(keyForCount, count);
    if (count == 0) {
      stringHelper.add(keyForDuration, "----");
    } else {
      stringHelper.add(keyForDuration, durationInMillis / count);
    }
  }

  @Override
  public String toString() {
    ToStringHelper stringHelper =
        MoreObjects.toStringHelper(this)
            .add("scannedTxns", scannedTransactions)
            .add("abortedTxns", abortedTransactions)
            .add("uncommittedTxns", uncommittedTransactions)
            .add("handledTxns", handledCommittedTransactions)
            .add("countOfDequeueTransaction", totalCountToDequeueFromTransactionQueue)
            .add("countOfReEnqueueTransaction", totalCountToReEnqueueFromTransactionQueue)
            .add("countOfDequeueUpdatedRecord", totalCountToDequeueFromUpdateRecordQueue)
            .add("countOfReEnqueueUpdatedRecord", totalCountToReEnqueueFromUpdateRecordQueue);

    addDuration(
        stringHelper,
        "countOfFetchTxns",
        "meanDurationInMillisToFetchTxns",
        totalCountToFetchTransaction.get(),
        totalDurationInMillisToFetchTransaction.get());

    addDuration(
        stringHelper,
        "countOfFetchBulkTxns",
        "meanDurationInMillisToFetchBulkTxns",
        totalCountToFetchBulkTransaction.get(),
        totalDurationInMillisToFetchBulkTransaction.get());

    addDuration(
        stringHelper,
        "countOfAppendValueToRecord",
        "meanDurationInMillisToAppendValueToRecord",
        totalCountToAppendValueToRecord.get(),
        totalDurationInMillisToAppendValueToRecord.get());

    addDuration(
        stringHelper,
        "countOfSetPrepTxIdInRecord",
        "meanDurationInMillisToSetPrepTxIdInRecord",
        totalCountToSetPrepTxIdInRecord.get(),
        totalDurationInMillisToSetPrepTxIdInRecord.get());

    addDuration(
        stringHelper,
        "countOfGetRecord",
        "meanDurationInMillisToGetRecord",
        totalCountToGetRecord.get(),
        totalDurationInMillisToGetRecord.get());

    addDuration(
        stringHelper,
        "countOfUpdateRecord",
        "meanDurationInMillisToUpdateRecord",
        totalCountToUpdateRecord.get(),
        totalDurationInMillisToUpdateRecord.get());

    return stringHelper
        .add("transactionQueue", transactionQueue)
        .add("recordQueue", updatedRecordQueue)
        .add("exceptionsInDistributor", exceptionCountInDistributor)
        .toString();
  }
}
