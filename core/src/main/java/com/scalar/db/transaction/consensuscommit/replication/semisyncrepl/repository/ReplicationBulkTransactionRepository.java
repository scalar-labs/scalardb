package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ReplicationBulkTransactionRepository {

  private final TypeReference<List<Transaction>> typeReferenceForTransactions =
      new TypeReference<List<Transaction>>() {};

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbBulkTransactionTable;

  public ReplicationBulkTransactionRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbBulkTransactionTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbBulkTransactionTable = replicationDbBulkTransactionTable;
  }

  public List<BulkTransaction> scan(int partitionId, int fetchTransactionSize)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbBulkTransactionTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("updated_at"))
                .limit(fetchTransactionSize)
                .build())) {
      return scan.all().stream()
          .map(
              result -> {
                String uniqueId = result.getText("unique_id");
                Instant updatedAt = Instant.ofEpochMilli(result.getBigInt("updated_at"));
                String serializedTransactions = result.getText("transactions");
                List<Transaction> transactions;
                try {
                  transactions =
                      objectMapper.readValue(serializedTransactions, typeReferenceForTransactions);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException("Failed to deserialize JSON into transactions", e);
                }
                return new BulkTransaction(partitionId, updatedAt, uniqueId, transactions);
              })
          .collect(Collectors.toList());
    }
  }

  private Put createBulkPutFromTransaction(int partitionId, Collection<Transaction> transactions) {
    // TODO: This can be compressed
    String serializedTransactions;
    try {
      serializedTransactions =
          objectMapper.writerFor(typeReferenceForTransactions).writeValueAsString(transactions);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize transactions into JSON string", e);
    }

    return Put.newBuilder()
        .namespace(replicationDbNamespace)
        .table(replicationDbBulkTransactionTable)
        .partitionKey(Key.ofInt("partition_id", partitionId))
        .clusteringKey(
            Key.newBuilder()
                .addBigInt("updated_at", System.currentTimeMillis())
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
    replicationDbStorage.delete(
        Delete.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbBulkTransactionTable)
            .partitionKey(Key.ofInt("partition_id", bulkTransaction.partitionId))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("updated_at", bulkTransaction.updatedAt.toEpochMilli())
                    .addText("unique_id", bulkTransaction.uniqueId)
                    .build())
            .build());
  }
}