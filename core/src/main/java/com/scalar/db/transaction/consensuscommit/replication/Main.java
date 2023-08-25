package com.scalar.db.transaction.consensuscommit.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.CommitHandler;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationTransactionRepository;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class Main {
  private final TransactionFactory transactionFactory;
  private final int numOfCustomers;

  public Main(TransactionFactory transactionFactory, int numOfCustomers) {
    this.transactionFactory = transactionFactory;
    this.numOfCustomers = numOfCustomers;
  }

  private void insertRecords() throws TransactionException {
    DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();
    for (int i = 0; i < numOfCustomers / 10; i++) {
      DistributedTransaction tx = transactionManager.begin();
      try {
        for (int j = 0; j < 10; j++) {
          int id = i * 10 + j;
          tx.put(
              Put.newBuilder()
                  .namespace("sample")
                  .table("customers")
                  .partitionKey(Key.ofInt("customer_id", id))
                  .textValue("name", "Customer: " + id)
                  .intValue("credit_limit", 10000)
                  .intValue("credit_total", id)
                  .build());
        }
        tx.commit();
      } catch (Exception e) {
        tx.abort();
        e.printStackTrace();
      }
    }
    transactionManager.close();
  }

  private void updateRecords() throws TransactionException {
    DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();
    for (int i = 0; i < numOfCustomers / 10; i++) {
      DistributedTransaction tx = transactionManager.begin();
      try {
        for (int j = 0; j < 10; j++) {
          int id = i * 10 + j;
          Optional<Result> result =
              tx.get(
                  Get.newBuilder()
                      .namespace("sample")
                      .table("customers")
                      .partitionKey(Key.ofInt("customer_id", id))
                      .build());
          if (!result.isPresent()) {
            throw new IllegalStateException("Customer not found: customer_id=" + id);
          }

          tx.put(
              Put.newBuilder()
                  .namespace("sample")
                  .table("customers")
                  .partitionKey(Key.ofInt("customer_id", id))
                  .condition(ConditionBuilder.putIfExists())
                  .intValue("credit_total", id)
                  .build());
        }
        tx.commit();
      } catch (Exception e) {
        tx.abort();
        e.printStackTrace();
      }
    }
    transactionManager.close();
  }

  private void deleteRecords() throws TransactionException {
    DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();
    for (int i = 0; i < numOfCustomers / 10; i++) {
      DistributedTransaction tx = transactionManager.begin();
      try {
        for (int j = 0; j < 2; j++) {
          int id = i * 10 + j * 5;
          Optional<Result> result =
              tx.get(
                  Get.newBuilder()
                      .namespace("sample")
                      .table("customers")
                      .partitionKey(Key.ofInt("customer_id", id))
                      .build());
          if (!result.isPresent()) {
            throw new IllegalStateException("Customer not found: customer_id=" + id);
          }

          tx.delete(
              Delete.newBuilder()
                  .namespace("sample")
                  .table("customers")
                  .partitionKey(Key.ofInt("customer_id", id))
                  .condition(ConditionBuilder.deleteIfExists())
                  .build());
        }
        tx.commit();
      } catch (TransactionException e) {
        tx.abort();
        e.printStackTrace();
      }
    }
    transactionManager.close();
  }

  private static final String ENV_VAR_SCALARDB_CONFIG = "DEMO_SCALARDB_CONFIG";
  private static final String ENV_VAR_REPLICATION_CONFIG = "DEMO_REPLICATION_CONFIG";
  private static final String ENV_VAR_NUM_OF_CUSTOMERS = "DEMO_NUM_OF_CUSTOMERS";
  private static final String ENV_VAR_UPDATE_LOOP = "DEMO_UPDATE_LOOP";

  public static void main(String[] args) throws IOException, TransactionException {
    String scalarDbConfigPath = System.getenv(ENV_VAR_SCALARDB_CONFIG);
    if (scalarDbConfigPath == null) {
      throw new IllegalArgumentException(
          "ScalarDB config file path isn't specified. key:" + ENV_VAR_SCALARDB_CONFIG);
    }

    String replicationDbConfigPath = System.getenv(ENV_VAR_REPLICATION_CONFIG);
    if (replicationDbConfigPath == null) {
      throw new IllegalArgumentException(
          "Replication config file path isn't specified. key:" + ENV_VAR_REPLICATION_CONFIG);
    }

    int numOfCustomers = 1000;
    if (System.getenv(ENV_VAR_NUM_OF_CUSTOMERS) != null) {
      numOfCustomers = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_CUSTOMERS));
    }

    int updateLoop = 2;
    if (System.getenv(ENV_VAR_UPDATE_LOOP) != null) {
      updateLoop = Integer.parseInt(System.getenv(ENV_VAR_UPDATE_LOOP));
    }

    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            new ObjectMapper(),
            "replication",
            "transactions");
    CommitHandler.replicationTransactionRepository.set(replicationTransactionRepository);

    TransactionFactory transactionFactory = TransactionFactory.create(scalarDbConfigPath);
    Main main = new Main(transactionFactory, numOfCustomers);
    System.out.println("Inserting customer records");
    Instant start = Instant.now();
    main.insertRecords();
    for (int i = 0; i < updateLoop; i++) {
      System.out.printf("Updating customer records (#%d)\n", i);
      main.updateRecords();
    }
    System.out.println("Deleting customer records");
    main.deleteRecords();
    System.out.println("Duration: " + Duration.between(start, Instant.now()));
  }
}
