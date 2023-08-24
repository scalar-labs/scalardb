package com.scalar.db.transaction.consensuscommit.replication;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
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

  public static void main(String[] args) throws IOException, TransactionException {
    if (args.length < 1) {
      throw new IllegalArgumentException("ScalarDB config file path isn't specified");
    }
    String scalarDbConfigPath = args[0];
    TransactionFactory transactionFactory = TransactionFactory.create(scalarDbConfigPath);

    int numOfCustomers = 5000;
    int updateLoop = 4;
    Main main = new Main(transactionFactory, numOfCustomers);
    Instant start = Instant.now();
    main.insertRecords();
    for (int i = 0; i < updateLoop; i++) {
      main.updateRecords();
    }
    main.deleteRecords();
    System.out.println("Duration: " + Duration.between(start, Instant.now()));
  }
}
