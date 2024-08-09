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
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationTransactionRepository {
  private static final Logger logger =
      LoggerFactory.getLogger(ReplicationTransactionRepository.class);

  private final TypeReference<List<WrittenTuple>> typeReferenceForWrittenTuples =
      new TypeReference<List<WrittenTuple>>() {};
  private final TypeReference<List<Transaction>> typeReferenceForTransactions =
      new TypeReference<List<Transaction>>() {};

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
                .ordering(Ordering.asc("updated_at"))
                .limit(fetchTransactionSize)
                .build())) {
      return scan.all().stream()
          .map(
              result -> {
                String transactionId = result.getText("transaction_id");
                Instant createdAt = Instant.ofEpochMilli(result.getBigInt("created_at"));
                Instant updatedAt = Instant.ofEpochMilli(result.getBigInt("updated_at"));
                List<WrittenTuple> writtenTuples;
                String writeSet = result.getText("write_set");
                try {
                  writtenTuples = objectMapper.readValue(writeSet, typeReferenceForWrittenTuples);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException("Failed to deserialize JSON into write tuples", e);
                }
                return new Transaction(
                    partitionId, createdAt, updatedAt, transactionId, writtenTuples);
              })
          .collect(Collectors.toList());
    }
  }

  private Put createPutFromTransaction(Transaction transaction) {
    String writeSet;
    try {
      writeSet =
          objectMapper
              .writerFor(typeReferenceForWrittenTuples)
              .writeValueAsString(transaction.writtenTuples);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize write tuples into JSON string", e);
    }

    return Put.newBuilder()
        .namespace(replicationDbNamespace)
        .table(replicationDbTransactionTable)
        .partitionKey(Key.ofInt("partition_id", transaction.partitionId))
        .clusteringKey(
            Key.newBuilder()
                .addBigInt("updated_at", transaction.updatedAt.toEpochMilli())
                .addText("transaction_id", transaction.transactionId)
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
        .value(TextColumn.of("write_set", writeSet))
        .build();
  }

  public void add(Transaction transaction) throws ExecutionException {
    logger.debug("[add]\n  transaction:{}\n", transaction);

    replicationDbStorage.put(createPutFromTransaction(transaction));
  }

  public void updateUpdatedAt(Transaction transaction) throws ExecutionException {
    Transaction newTxn =
        new Transaction(
            transaction.partitionId,
            transaction.createdAt,
            Instant.now(),
            transaction.transactionId,
            transaction.writtenTuples);

    logger.debug("[updateUpdatedAt]\n  transaction:{}\n", transaction);

    add(newTxn);
    // It's okay if deleting the old record remains
    delete(transaction);
  }

  public void delete(Transaction transaction) throws ExecutionException {
    logger.debug("[delete]\n  transaction:{}\n", transaction);

    replicationDbStorage.delete(
        Delete.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbTransactionTable)
            .partitionKey(Key.ofInt("partition_id", transaction.partitionId))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("updated_at", transaction.updatedAt.toEpochMilli())
                    .addText("transaction_id", transaction.transactionId)
                    .build())
            .build());
  }
}
