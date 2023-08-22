package com.scalar.db.transaction.consensuscommit.replication.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.model.WrittenTuple;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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

  public void add(
      int partitionId, long createdAt, String transactionId, Collection<WrittenTuple> writtenTuples)
      throws ExecutionException {
    String writeSet;
    try {
      writeSet = objectMapper.writerFor(typeReference).writeValueAsString(writtenTuples);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to convert write tuples into JSON string", e);
    }

    Buildable builder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbTransactionTable)
            .partitionKey(Key.ofInt("partition_id", partitionId))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("created_at", createdAt)
                    .addText("transaction_id", transactionId)
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
