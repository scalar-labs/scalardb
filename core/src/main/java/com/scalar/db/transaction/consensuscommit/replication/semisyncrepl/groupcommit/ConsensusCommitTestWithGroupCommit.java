package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConsensusCommitTestWithGroupCommit implements AutoCloseable {
  private final Random random = new Random();
  private final int numOfCustomers;
  private final int numOfRequests;
  private final int numOfOpsPerTx;
  private final ExecutorService executorService;
  private final DistributedTransactionManager transactionManager;

  public ConsensusCommitTestWithGroupCommit(
      TransactionFactory transactionFactory,
      int numOfThreads,
      int numOfCustomers,
      int numOfRequests,
      int numOfOpsPerTx) {
    this.numOfCustomers = numOfCustomers;
    this.numOfRequests = numOfRequests;
    this.numOfOpsPerTx = numOfOpsPerTx;
    transactionManager = transactionFactory.getTransactionManager();
    executorService = Executors.newFixedThreadPool(numOfThreads);
  }

  @FunctionalInterface
  interface TransactionTask<T> {
    T run(DistributedTransaction transaction) throws TransactionException;
  }

  private <T> Future<T> wrapTransaction(ExecutorService executorService, TransactionTask<T> task) {
    return executorService.submit(
        () -> {
          int counter = 0;
          while (true) {
            DistributedTransaction tx = transactionManager.begin();
            try {
              T result = task.run(tx);
              tx.commit();
              return result;
            } catch (UnknownTransactionStatusException e) {
              System.out.println(e.getMessage());
              throw e;
            } catch (CrudConflictException
                | CommitConflictException
                | PreparationConflictException
                | ValidationConflictException e) {
              tx.abort();
              System.out.println(e.getMessage());
              // Retry
            } catch (Exception e) {
              tx.abort();
              System.out.println(e.getMessage());
              if (++counter >= 20) {
                throw e;
              }
              TimeUnit.SECONDS.sleep(2);
            }
          }
        });
  }

  private void loadRecords() throws ExecutionException, InterruptedException {
    List<Future<Void>> futures = new ArrayList<>(numOfCustomers);
    for (int i = 0; i < numOfCustomers; i++) {
      int id = i;
      TransactionTask<Void> task =
          tx -> {
            Optional<Result> result =
                tx.get(
                    Get.newBuilder()
                        .namespace("sample")
                        .table("customers")
                        .partitionKey(Key.ofInt("customer_id", id))
                        .build());
            if (result.isPresent()) {
              tx.put(
                  Put.newBuilder()
                      .namespace("sample")
                      .table("customers")
                      .partitionKey(Key.ofInt("customer_id", id))
                      .condition(ConditionBuilder.putIfExists())
                      .textValue("name", "Customer: " + id)
                      .intValue("credit_limit", 10000)
                      .intValue("credit_total", 0)
                      .build());
            } else {
              tx.put(
                  Put.newBuilder()
                      .namespace("sample")
                      .table("customers")
                      .partitionKey(Key.ofInt("customer_id", id))
                      .textValue("name", "Customer: " + id)
                      .intValue("credit_limit", 10000)
                      .intValue("credit_total", 0)
                      .build());
            }
            return null;
          };
      futures.add(wrapTransaction(executorService, task));
    }

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private void updateRecords() throws ExecutionException, InterruptedException {
    List<Future<Void>> futures = new ArrayList<>(numOfCustomers);
    for (int i = 0; i < numOfRequests; i++) {
      TransactionTask<Void> task =
          tx -> {
            int startingId = random.nextInt(numOfCustomers);
            for (int j = 0; j < numOfOpsPerTx; j++) {
              int id = (startingId + j) % numOfCustomers;
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
                      .intValue("credit_total", result.get().getInt("credit_total") + 1)
                      .build());
            }
            return null;
          };

      futures.add(wrapTransaction(executorService, task));
    }

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private long verifyRecords() throws ExecutionException, InterruptedException {
    List<Future<Integer>> futures = new ArrayList<>(numOfCustomers);
    for (int i = 0; i < numOfCustomers; i++) {
      int id = i;
      TransactionTask<Integer> task =
          tx -> {
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
            return result.get().getInt("credit_total");
          };

      futures.add(wrapTransaction(executorService, task));
    }

    long sum = 0;
    for (Future<Integer> future : futures) {
      sum += future.get();
    }
    return sum;
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

  private static final String ENV_VAR_SCALARDB_CONFIG = "GC_TEST_SCALARDB_CONFIG";
  private static final String ENV_VAR_NUM_OF_CUSTOMERS = "GC_TEST_NUM_OF_CUSTOMERS";
  private static final String ENV_VAR_NUM_OF_REQUESTS = "GC_TEST_NUM_OF_REQUESTS";
  private static final String ENV_VAR_NUM_OF_OPS_PER_TX = "GC_TEST_NUM_OF_OPS_PER_TX";
  private static final String ENV_VAR_NUM_OF_THREADS = "GC_TEST_NUM_OF_THREADS";

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

    int numOfRequests = 20000;
    if (System.getenv(ENV_VAR_NUM_OF_REQUESTS) != null) {
      numOfRequests = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_REQUESTS));
    }

    int numOfOpsPerTx = 4;
    if (System.getenv(ENV_VAR_NUM_OF_OPS_PER_TX) != null) {
      numOfOpsPerTx = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_OPS_PER_TX));
    }

    int numOfThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_THREADS) != null) {
      numOfThreads = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_THREADS));
    }

    TransactionFactory transactionFactory = TransactionFactory.create(scalarDbConfigPath);
    try (ConsensusCommitTestWithGroupCommit main =
        new ConsensusCommitTestWithGroupCommit(
            transactionFactory, numOfThreads, numOfCustomers, numOfRequests, numOfOpsPerTx)) {
      Instant start = Instant.now();
      System.out.printf("Inserting %d customer records\n", numOfCustomers);
      main.loadRecords();
      System.out.printf("Updating %d customer records\n", numOfCustomers);
      main.updateRecords();
      System.out.printf("Verifying %d customer records\n", numOfCustomers);
      long sum = main.verifyRecords();
      System.out.printf(
          "The sum of `credit_total`:%d. Expected:%d\n", sum, numOfRequests * numOfOpsPerTx);
      System.out.printf("Duration: %s\n", Duration.between(start, Instant.now()));
    }
  }
}
