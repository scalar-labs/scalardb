package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.ScanBuilder.BuildableScan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationBulkTransactionRepository {
  private static final Logger logger =
      LoggerFactory.getLogger(ReplicationBulkTransactionRepository.class);

  private final TypeReference<List<Transaction>> typeReferenceForTransactions =
      new TypeReference<List<Transaction>>() {};

  private final DistributedStorage replicationDbStorage;
  private final String replicationDbNamespace;
  private final String replicationDbBulkTransactionTable;

  public ReplicationBulkTransactionRepository(
      DistributedStorage replicationDbStorage,
      String replicationDbNamespace,
      String replicationDbBulkTransactionTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbBulkTransactionTable = replicationDbBulkTransactionTable;
  }

  public static class ScanResult {
    public final Long nextScanTimestampMillis;
    public final List<BulkTransaction> bulkTransactions;

    public ScanResult(Long nextScanTimestampMillis, List<BulkTransaction> bulkTransactions) {
      this.nextScanTimestampMillis = nextScanTimestampMillis;
      this.bulkTransactions = bulkTransactions;
    }
  }

  /*
  public List<BulkTransaction> scan(int partitionId, int fetchTransactionSize)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbBulkTransactionTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("created_at"))
                .limit(fetchTransactionSize)
                .build())) {
      return scan.all().stream()
          .map(
              result -> {
                String uniqueId = result.getText("unique_id");
                Instant updatedAt = Instant.ofEpochMilli(result.getBigInt("created_at"));
                String serializedTransactions = result.getText("transactions");
                List<Transaction> transactions;
                transactions = JSON.parseArray(serializedTransactions, Transaction.class);
                return new BulkTransaction(partitionId, updatedAt, uniqueId, transactions);
              })
          .collect(Collectors.toList());
    }
  }
   */
  public ScanResult scan(int partitionId, int fetchTransactionSize, Long scanStartTsMillis)
      throws ExecutionException, IOException {
    BuildableScan builder =
        Scan.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbBulkTransactionTable)
            .partitionKey(Key.ofInt("partition_id", partitionId))
            .ordering(Ordering.asc("created_at"));

    if (scanStartTsMillis != null) {
      builder.start(Key.ofBigInt("created_at", scanStartTsMillis));
    }

    builder.limit(fetchTransactionSize);

    try (Scanner scan = replicationDbStorage.scan(builder.build())) {
      Long lastTimestampMillis = null;
      List<BulkTransaction> bulkTransactions = new ArrayList<>();

      for (Result result : scan.all()) {
        String uniqueId = result.getText("unique_id");
        Instant createdAt = Instant.ofEpochMilli(result.getBigInt("created_at"));
        String serializedTransactions = result.getText("transactions");
        List<Transaction> transactions = JSON.parseArray(serializedTransactions, Transaction.class);
        bulkTransactions.add(new BulkTransaction(partitionId, createdAt, uniqueId, transactions));

        if (lastTimestampMillis == null || lastTimestampMillis < createdAt.toEpochMilli()) {
          lastTimestampMillis = createdAt.toEpochMilli();
        }
      }

      return new ScanResult(lastTimestampMillis, bulkTransactions);
    }
  }

  private Put createBulkPutFromTransaction(int partitionId, Collection<Transaction> transactions) {
    // TODO: This can be compressed
    String serializedTransactions = JSON.toJSONString(transactions);

    return Put.newBuilder()
        .namespace(replicationDbNamespace)
        .table(replicationDbBulkTransactionTable)
        .partitionKey(Key.ofInt("partition_id", partitionId))
        .clusteringKey(
            Key.newBuilder()
                .addBigInt("created_at", System.currentTimeMillis())
                // TODO: Make unique_id be passed from outside for retry
                .addText("unique_id", UUID.randomUUID().toString())
                .build())
        .value(TextColumn.of("transactions", serializedTransactions))
        .build();
  }

  public void add(List<Transaction> transactions) throws ExecutionException {
    if (transactions.isEmpty()) {
      return;
    }

    replicationDbStorage.put(
        createBulkPutFromTransaction(
            transactions.stream().findFirst().get().partitionId, transactions));
  }

  public void delete(BulkTransaction bulkTransaction) throws ExecutionException {
    try {
      replicationDbStorage.delete(
          Delete.newBuilder()
              .namespace(replicationDbNamespace)
              .table(replicationDbBulkTransactionTable)
              .partitionKey(Key.ofInt("partition_id", bulkTransaction.partitionId))
              .clusteringKey(
                  Key.newBuilder()
                      .addBigInt("created_at", bulkTransaction.createdAt.toEpochMilli())
                      .addText("unique_id", bulkTransaction.uniqueId)
                      .build())
              .build());
    } catch (ExecutionException e) {
      // WORKAROUND: https://scalar-labs.atlassian.net/browse/DLT-16038
      if (e.getMessage().contains("Resource Not Found")) {
        logger.info("Skipping a known issue");
      } else {
        throw e;
      }
    }
  }
}
