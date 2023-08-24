package com.scalar.db.transaction.consensuscommit.replication.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.model.WrittenTuple;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationTransactionRepository {
  private static final Logger logger =
      LoggerFactory.getLogger(ReplicationTransactionRepository.class);
  private final TypeReference<List<WrittenTuple>> typeReference =
      new TypeReference<List<WrittenTuple>>() {};

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionTable;

  public ReplicationTransactionRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbTransactionTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionTable = replicationDbTransactionTable;
  }

  public List<Transaction> scan(int partitionId, int fetchTransactionSize)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbTransactionTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("created_at"))
                .limit(fetchTransactionSize)
                .build())) {
      return scan.all().stream()
          .map(
              result -> {
                String transactionId = result.getText("transaction_id");
                Instant createdAt = Instant.ofEpochMilli(result.getBigInt("created_at"));
                List<WrittenTuple> writtenTuples;
                String writeSet = result.getText("write_set");
                try {
                  writtenTuples = objectMapper.readValue(writeSet, typeReference);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(
                      "Failed to deserialize write tuples into JSON string", e);
                }
                return new Transaction(partitionId, createdAt, transactionId, writtenTuples);
              })
          .collect(Collectors.toList());
    }
  }

  public void add(Transaction transaction) throws ExecutionException {
    String writeSet;
    try {
      writeSet =
          objectMapper.writerFor(typeReference).writeValueAsString(transaction.writtenTuples());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize write tuples into JSON string", e);
    }

    Buildable builder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbTransactionTable)
            .partitionKey(Key.ofInt("partition_id", transaction.partitionId()))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("created_at", transaction.createdAt().toEpochMilli())
                    .addText("transaction_id", transaction.transactionId())
                    .build())
            // TODO: Revisit here
            /*
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("transaction_id")
                            .isNotEqualToText(extractor.txId()))
                    .build())
             */
            // TODO: Revisit here as this would be slow. Schemaful serialization should be better
            .value(TextColumn.of("write_set", writeSet));

    replicationDbStorage.put(builder.build());
  }

  public void delete(Transaction transaction) throws ExecutionException {
    replicationDbStorage.delete(
        Delete.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbTransactionTable)
            .partitionKey(Key.ofInt("partition_id", transaction.partitionId()))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("created_at", transaction.createdAt().toEpochMilli())
                    .addText("transaction_id", transaction.transactionId())
                    .build())
            .build());
  }
}
