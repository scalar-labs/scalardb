package com.scalar.db.transaction.consensuscommit.replication.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.semisync.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Column;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Distributor implements Closeable {
  private final ExecutorService executorService;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionsTable;
  private final String replicationDbRecordsTable;
  private final int replicationDbPartitionSize;
  private final int replicationDbThreadSize;
  private final int fetchTransactionSize;
  private final DistributedStorage storage;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final TypeReference<List<WrittenTuple>> typeRefForWrittenTupleInTransactions =
      new TypeReference<List<WrittenTuple>>() {};
  private final TypeReference<List<Value>> typeRefForValueInRecords =
      new TypeReference<List<Value>>() {};

  public static class Value {
    public final String prevTxId;
    public final String txId;
    public final String type;
    public final Collection<Column<?>> columns;

    public Value(
        @JsonProperty("prevTxId") String prevTxId,
        @JsonProperty("txId") String txId,
        @JsonProperty("type") String type,
        @JsonProperty("columns") Collection<Column<?>> columns) {
      this.prevTxId = prevTxId;
      this.txId = txId;
      this.type = type;
      this.columns = columns;
    }
  }

  public Distributor(
      String replicationDbNamespace,
      String replicationDbTransactionsTable,
      String replicationDbRecordsTable,
      int replicationDbPartitionSize,
      int replicationDbThreadSize,
      int fetchTransactionSize,
      Properties replicationDbProperties) {
    if (replicationDbPartitionSize % replicationDbThreadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              replicationDbPartitionSize, replicationDbThreadSize));
    }
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionsTable = replicationDbTransactionsTable;
    this.replicationDbRecordsTable = replicationDbRecordsTable;
    this.replicationDbPartitionSize = replicationDbPartitionSize;
    this.replicationDbThreadSize = replicationDbThreadSize;
    this.fetchTransactionSize = fetchTransactionSize;
    this.executorService =
        Executors.newFixedThreadPool(
            replicationDbThreadSize,
            new ThreadFactoryBuilder().setNameFormat("log-distributor-%d").build());
    this.storage = StorageFactory.create(replicationDbProperties).getStorage();
  }

  private Key createKey(
      String namespace,
      String table,
      com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key partitionKey,
      com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key clusteringKey) {
    String keys =
        Streams.concat(
                partitionKey.columns.stream().map(column -> column.name + "=" + column.value),
                clusteringKey.columns.stream().map(column -> column.name + "=" + column.value))
            .filter(name -> name != null && !name.isEmpty())
            .collect(Collectors.joining(":"));
    return Key.newBuilder()
        .addText("namespace", namespace)
        .addText("table", table)
        .addText("keys", keys)
        .build();
  }

  private void fetchAndHandleTransactionRecord(int partitionId) {
    try (Scanner scan =
        storage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbTransactionsTable)
                // TODO: Provision for performance
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("created_at"))
                .limit(fetchTransactionSize)
                .build())) {
      for (Result result : scan.all()) {
        String transactionId = result.getText("transaction_id");
        long createdAt = result.getBigInt("created_at");
        String writeSet = result.getText("write_set");
        List<WrittenTuple> writtenTuples =
            objectMapper.readValue(writeSet, typeRefForWrittenTupleInTransactions);
        for (WrittenTuple writtenTuple : writtenTuples) {
          Key key =
              createKey(
                  writtenTuple.namespace,
                  writtenTuple.table,
                  writtenTuple.partitionKey,
                  writtenTuple.clusteringKey);
          Optional<Result> existingRecord =
              storage.get(
                  Get.newBuilder()
                      .namespace(replicationDbNamespace)
                      .table(replicationDbRecordsTable)
                      // TODO: Provision for performance
                      .partitionKey(key)
                      .build());

          Buildable putBuilder =
              Put.newBuilder()
                  .namespace(replicationDbNamespace)
                  .table(replicationDbRecordsTable)
                  .partitionKey(key);

          Value newValue;

          if (writtenTuple instanceof InsertedTuple) {
            InsertedTuple tuple = (InsertedTuple) writtenTuple;
            newValue = new Value(null, transactionId, "insert", tuple.columns);
          } else if (writtenTuple instanceof UpdatedTuple) {
            UpdatedTuple tuple = (UpdatedTuple) writtenTuple;
            newValue = new Value(tuple.prevTxId, transactionId, "update", tuple.columns);
          } else if (writtenTuple instanceof DeletedTuple) {
            DeletedTuple tuple = (DeletedTuple) writtenTuple;
            newValue = new Value(tuple.prevTxId, transactionId, "delete", null);
          } else {
            throw new AssertionError();
          }

          if (existingRecord.isPresent()) {
            List<Value> values =
                objectMapper.readValue(
                    existingRecord.get().getText("values"), typeRefForValueInRecords);
            values.add(newValue);

            putBuilder
                .condition(ConditionBuilder.putIfExists())
                .textValue("values", objectMapper.writeValueAsString(values));
          } else {
            putBuilder
                .condition(ConditionBuilder.putIfNotExists())
                .textValue(
                    "values", objectMapper.writeValueAsString(Collections.singletonList(newValue)));
          }
          storage.put(putBuilder.build());
          storage.delete(
              Delete.newBuilder()
                  .namespace(replicationDbNamespace)
                  .table(replicationDbTransactionsTable)
                  .partitionKey(Key.ofInt("partition_id", partitionId))
                  .clusteringKey(
                      Key.newBuilder()
                          .addBigInt("created_at", createdAt)
                          .addText("transaction_id", transactionId)
                          .build())
                  .build());
        }
      }
    } catch (Throwable e) {
      // FIXME: Error handling
      e.printStackTrace();
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }

  public Distributor run() {
    for (int i = 0; i < replicationDbThreadSize; i++) {
      int startPartitionId = i;
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              for (int partitionId = startPartitionId;
                  partitionId < replicationDbPartitionSize;
                  partitionId += replicationDbThreadSize) {
                fetchAndHandleTransactionRecord(partitionId);
              }
            }
          });
    }
    return this;
  }

  @Override
  public void close() {
    executorService.shutdown();
    // TODO: Make this configurable
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    // FIXME: This is only for PoC.
    Properties replicationDbProps = new Properties();
    replicationDbProps.put("scalar.db.storage", "jdbc");
    replicationDbProps.put("scalar.db.contact_points", "jdbc:mysql://localhost/replication");
    replicationDbProps.put("scalar.db.username", "root");
    replicationDbProps.put("scalar.db.password", "mysql");
    Distributor distributor =
        new Distributor("replication", "transactions", "records", 256, 1, 1, replicationDbProps)
            .run();
    Runtime.getRuntime().addShutdownHook(new Thread(distributor::close));
  }
}
