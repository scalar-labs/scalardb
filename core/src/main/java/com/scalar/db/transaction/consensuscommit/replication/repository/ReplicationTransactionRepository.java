package com.scalar.db.transaction.consensuscommit.replication.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.model.Record.Value;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationTransactionRepository {
  private static final Logger logger =
      LoggerFactory.getLogger(ReplicationTransactionRepository.class);
  private static final TypeReference<Set<Value>> typeRefForValueInRecords =
      new TypeReference<Set<Value>>() {};

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionTable;
  private final int fetchTransactionSize;

  public ReplicationTransactionRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbTransactionTable,
      int fetchTransactionSize) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionTable = replicationDbTransactionTable;
    this.fetchTransactionSize = fetchTransactionSize;
  }

  public Key createKey(
      String namespace,
      String table,
      com.scalar.db.transaction.consensuscommit.replication.model.Key partitionKey,
      com.scalar.db.transaction.consensuscommit.replication.model.Key clusteringKey)
      throws JsonProcessingException {
    return Key.newBuilder()
        .addText("namespace", namespace)
        .addText("table", table)
        .addText("pk", objectMapper.writeValueAsString(partitionKey))
        .addText("ck", objectMapper.writeValueAsString(clusteringKey))
        .build();
  }

  public List<Result> scan(int partitionId) throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbTransactionTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("created_at"))
                .limit(fetchTransactionSize)
                .build())) {
      return scan.all();
    }
  }

  public void delete(int partitionId, long createdAt, String transactionId)
      throws ExecutionException {
    replicationDbStorage.delete(
        Delete.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbTransactionTable)
            .partitionKey(Key.ofInt("partition_id", partitionId))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("created_at", createdAt)
                    .addText("transaction_id", transactionId)
                    .build())
            .build());
  }
}
