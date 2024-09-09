package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
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

public class ReplicationTransactionRepository {

  private final TypeReference<List<WrittenTuple>> typeReferenceForWrittenTuples =
      new TypeReference<List<WrittenTuple>>() {};

  private final DistributedStorage replicationDbStorage;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionTable;

  public ReplicationTransactionRepository(
      DistributedStorage replicationDbStorage,
      String replicationDbNamespace,
      String replicationDbTransactionTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionTable = replicationDbTransactionTable;
  }

  public List<Transaction> scan(int partitionId, int fetchTransactionSize, long scanStartTsMillis)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbTransactionTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("updated_at"))
                .end(Key.ofBigInt("updated_at", scanStartTsMillis))
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
                writtenTuples = JSON.parseObject(writeSet, typeReferenceForWrittenTuples);
                return new Transaction(
                    partitionId, createdAt, updatedAt, transactionId, writtenTuples);
              })
          .collect(Collectors.toList());
    }
  }

  private Put createPutFromTransaction(Transaction transaction) {
    String writeSet = JSON.toJSONString(transaction.writtenTuples);

    return Put.newBuilder()
        .namespace(replicationDbNamespace)
        .table(replicationDbTransactionTable)
        .partitionKey(Key.ofInt("partition_id", transaction.partitionId))
        .clusteringKey(
            Key.newBuilder()
                .addBigInt("updated_at", transaction.updatedAt.toEpochMilli())
                .addText("transaction_id", transaction.transactionId)
                .build())
        .value(TextColumn.of("write_set", writeSet))
        .build();
  }

  public void add(Transaction transaction) throws ExecutionException {
    replicationDbStorage.put(createPutFromTransaction(transaction));
  }

  public Transaction updateUpdatedAt(Transaction transaction) throws ExecutionException {
    Instant now = Instant.now();
    Transaction updatedTransaction =
        new Transaction(
            transaction.partitionId,
            transaction.createdAt,
            now,
            transaction.transactionId,
            transaction.writtenTuples);

    add(updatedTransaction);
    // It's okay if deleting the old record remains
    delete(transaction);

    return updatedTransaction;
  }

  public void delete(Transaction transaction) throws ExecutionException {
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
