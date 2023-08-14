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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DemoClient implements AutoCloseable {
  private final int numOfCustomers;
  private final ExecutorService executorService;
  private final DistributedTransactionManager transactionManager;

  public DemoClient(TransactionFactory transactionFactory, int numOfThreads, int numOfCustomers) {
    this.numOfCustomers = numOfCustomers;
    transactionManager = transactionFactory.getTransactionManager();
    executorService = Executors.newFixedThreadPool(numOfThreads);
  }

  private void insertRecords() throws ExecutionException, InterruptedException {
    List<Future<Void>> futures = new ArrayList<>(numOfCustomers);
    for (int i = 0; i < numOfCustomers / 10; i++) {
      int finalI = i;
      futures.add(
          executorService.submit(
              () -> {
                DistributedTransaction tx = transactionManager.begin();
                try {
                  for (int j = 0; j < 10; j++) {
                    int id = finalI * 10 + j;
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
                return null;
              }));
    }

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private void updateRecords() throws ExecutionException, InterruptedException {
    List<Future<Void>> futures = new ArrayList<>(numOfCustomers);
    for (int i = 0; i < numOfCustomers / 10; i++) {
      int finalI = i;
      futures.add(
          executorService.submit(
              () -> {
                DistributedTransaction tx = transactionManager.begin();
                try {
                  for (int j = 0; j < 10; j++) {
                    int id = finalI * 10 + j;
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
                            .intValue("credit_total", result.get().getInt("credit_total") * 2)
                            .build());
                  }
                  tx.commit();
                } catch (Exception e) {
                  tx.abort();
                  e.printStackTrace();
                }
                return null;
              }));
    }

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private void deleteRecords() throws ExecutionException, InterruptedException {
    List<Future<Void>> futures = new ArrayList<>(numOfCustomers);
    for (int i = 0; i < numOfCustomers / 10; i++) {
      int finalI = i;
      futures.add(
          executorService.submit(
              () -> {
                DistributedTransaction tx = transactionManager.begin();
                try {
                  for (int j = 0; j < 2; j++) {
                    int id = finalI * 10 + j * 5;
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
                return null;
              }));

      for (Future<Void> future : futures) {
        future.get();
      }
    }
  }

  @Override
  public void close() {
    transactionManager.close();
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static final String ENV_VAR_SCALARDB_CONFIG = "DEMO_SCALARDB_CONFIG";
  private static final String ENV_VAR_NUM_OF_CUSTOMERS = "DEMO_NUM_OF_CUSTOMERS";
  private static final String ENV_VAR_NUM_OF_THREADS = "DEMO_NUM_OF_THREADS";
  private static final String ENV_VAR_UPDATE_LOOP = "DEMO_UPDATE_LOOP";

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    String scalarDbConfigPath = System.getenv(ENV_VAR_SCALARDB_CONFIG);
    if (scalarDbConfigPath == null) {
      throw new IllegalArgumentException(
          "ScalarDB config file path isn't specified. key:" + ENV_VAR_SCALARDB_CONFIG);
    }

    int numOfCustomers = 2000;
    if (System.getenv(ENV_VAR_NUM_OF_CUSTOMERS) != null) {
      numOfCustomers = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_CUSTOMERS));
    }

    int numOfThreads = 4;
    if (System.getenv(ENV_VAR_NUM_OF_THREADS) != null) {
      numOfThreads = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_THREADS));
    }

    int updateLoop = 2;
    if (System.getenv(ENV_VAR_UPDATE_LOOP) != null) {
      updateLoop = Integer.parseInt(System.getenv(ENV_VAR_UPDATE_LOOP));
    }

    TransactionFactory transactionFactory = TransactionFactory.create(scalarDbConfigPath);
    try (DemoClient main = new DemoClient(transactionFactory, numOfThreads, numOfCustomers)) {
      System.out.printf("Inserting %d customer records\n", numOfCustomers);
      Instant start = Instant.now();
      main.insertRecords();
      for (int i = 0; i < updateLoop; i++) {
        System.out.printf("Updating %d customer records (#%d)\n", numOfCustomers, i);
        main.updateRecords();
      }
      System.out.printf("Deleting %d customer records\n", numOfCustomers / 5);
      main.deleteRecords();
      System.out.println("Duration: " + Duration.between(start, Instant.now()));
    }
  }
}
