package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.scalar.db.io.Key;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Metrics {
  public final AtomicInteger scannedTransactions = new AtomicInteger();
  public final AtomicInteger uncommittedTransactions = new AtomicInteger();
  public final AtomicInteger handledCommittedTransactions = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToFetchTransaction = new AtomicLong();
  public final AtomicInteger totalCountToFetchTransaction = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToFetchBulkTransaction = new AtomicLong();
  public final AtomicInteger totalCountToFetchBulkTransaction = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToAppendValueToRecord = new AtomicLong();
  public final AtomicInteger totalCountToAppendValueToRecord = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToSetPrepTxIdInRecord = new AtomicLong();
  public final AtomicInteger totalCountToGetRecord = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToGetRecord = new AtomicLong();
  public final AtomicInteger totalCountToSetPrepTxIdInRecord = new AtomicInteger();
  public final AtomicLong totalDurationInMillisToUpdateRecord = new AtomicLong();
  public final AtomicInteger totalCountToUpdateRecord = new AtomicInteger();
  public final AtomicInteger exceptionCountInDistributor = new AtomicInteger();

  private final Queue<Key> recordWriterQueue;

  public Metrics(Queue<Key> recordWriterQueue) {
    this.recordWriterQueue = recordWriterQueue;
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
            .add("uncommittedTxns", uncommittedTransactions)
            .add("handledTxns", handledCommittedTransactions);

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
        .add("recordWriterQueueLength", recordWriterQueue.size())
        .add("exceptionsInDistributor", exceptionCountInDistributor)
        .toString();
  }
}
