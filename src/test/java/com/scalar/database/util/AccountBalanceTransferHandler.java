package com.scalar.database.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.database.api.Consistency;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.DistributedTransaction;
import com.scalar.database.api.DistributedTransactionManager;
import com.scalar.database.api.Get;
import com.scalar.database.api.Put;
import com.scalar.database.api.Result;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.exception.transaction.CommitException;
import com.scalar.database.exception.transaction.CrudException;
import com.scalar.database.exception.transaction.UnknownTransactionStatusException;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * CREATE KEYSPACE transfer WITH replication = {'class': 'SimpleStrategy', 'replication_factor':
 * 'x'} CREATE KEYSPACE coordinator WITH replication = {'class': 'SimpleStrategy',
 * 'replication_factor': 'x'}
 */

/**
 * CREATE TABLE transfer.tx_transfer (account_id int, balance int, tx_id text, tx_version int,
 * tx_state int, tx_prepared_at bigint, tx_committed_at bigint, before_account_id int,
 * before_balance int, before_tx_id text, before_tx_version int, before_tx_state int,
 * before_tx_prepared_at bigint, before_tx_committed_at bigint, PRIMARY KEY ((account_id)))
 */
public class AccountBalanceTransferHandler {
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final long SLEEP_BASE_MILLIS = 100;
  private static final int DEFAULT_CONCURRENCY = 8;
  private static final int NUM_TYPES = 2;
  private static final int CONCURRENCY = 8;
  private static final int DEFAULT_INITIAL_BALANCE = 10000;
  private static final int NUM_PER_TX = 100;

  private final DatabaseConfig config;
  private final DistributedStorage storage;
  private final DistributedTransactionManager manager;
  private final TransferContext context;
  private final String namespace;
  private final String table;

  public AccountBalanceTransferHandler(
      DatabaseConfig config, TransferContext context, String namespace, String table) {
    this.config = config;
    this.context = checkNotNull(context);
    this.namespace = namespace;
    this.table = table;
    this.storage = TransactionUtility.prepareStorage(config);
    this.manager = TransactionUtility.prepareTransactionManager(storage, namespace, table);
  }

  public void prepareTables() {
    TransactionUtility.prepareCoordinatorTable(config);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(ACCOUNT_ID, "int");
    attributes.put(ACCOUNT_TYPE, "int");
    attributes.put(BALANCE, "int");
    TransactionUtility.prepareUserTable(
        config,
        namespace,
        table,
        Arrays.asList(ACCOUNT_ID),
        Arrays.asList(ACCOUNT_TYPE),
        attributes);
  }

  public void populateRecords() {
    System.out.println("insert initial values ... ");

    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, DEFAULT_CONCURRENCY)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new PopulationRunner(i).run();
                      },
                      es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    System.out.println("all records have been inserted");
  }

  public void runTransfer(int concurrency) {
    System.out.println("running transfer programs ... ");

    AtomicBoolean running = new AtomicBoolean(true);
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new TransferRunner(i).run(running);
                      },
                      es);
              futures.add(future);
            });

    try {
      TimeUnit.SECONDS.sleep(context.getRunTime() + context.getRampTime());
      running.set(false);
    } catch (InterruptedException e) {
      // ignored
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    System.out.println("transfer programs finished");
  }

  public List<Result> readRecordsWithRetry() {
    System.out.println("reading latest records ...");
    int i = 0;
    while (true) {
      if (i >= 10) {
        throw new RuntimeException("some records can't be recovered");
      }
      try {
        return readRecords();
      } catch (Exception e) {
        ++i;
        TransactionUtility.exponentialBackoff(i);
      }
    }
  }

  private List<Result> readRecords() {
    List<Result> results = new ArrayList<>();
    DistributedTransaction transaction = manager.start();
    IntStream.range(0, context.getNumAccounts())
        .forEach(
            i -> {
              IntStream.range(0, NUM_TYPES)
                  .forEach(
                      j -> {
                        Key partitionKey = new Key(new IntValue(ACCOUNT_ID, i));
                        Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, j));
                        Get get = new Get(partitionKey, clusteringKey);
                        try {
                          transaction.get(get).ifPresent(r -> results.add(r));
                        } catch (CrudException e) {
                          throw new RuntimeException(e);
                        }
                      });
            });
    return results;
  }

  public int calcTotalInitialBalance() {
    return context.getNumAccounts() * NUM_TYPES * DEFAULT_INITIAL_BALANCE;
  }

  public int calcTotalBalance(List<Result> results) {
    return results.stream().mapToInt(r -> ((IntValue) r.getValue(BALANCE).get()).get()).sum();
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int numPerThread = (context.getNumAccounts() + CONCURRENCY - 1) / CONCURRENCY;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), context.getNumAccounts());
      IntStream.range(0, (numPerThread + NUM_PER_TX - 1) / NUM_PER_TX)
          .forEach(
              i -> {
                int startId = start + NUM_PER_TX * i;
                int endId = Math.min(start + NUM_PER_TX * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      int retries = 0;
      while (true) {
        if (retries++ > 10) {
          throw new RuntimeException("population failed repeatedly!");
        }
        DistributedTransaction transaction = manager.start();
        IntStream.range(startId, endId)
            .forEach(
                i -> {
                  IntStream.range(0, NUM_TYPES)
                      .forEach(
                          j -> {
                            Key partitionKey = new Key(new IntValue(ACCOUNT_ID, i));
                            Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, j));
                            Put put =
                                new Put(partitionKey, clusteringKey)
                                    .withConsistency(Consistency.LINEARIZABLE)
                                    .withValue(new IntValue(BALANCE, DEFAULT_INITIAL_BALANCE));
                            transaction.put(put);
                          });
                });
        try {
          transaction.commit();
          break;
        } catch (Exception e) {
          // ignored
        }
      }
    }
  }

  private class TransferRunner {
    private final Random random;
    private String currentTxId = null;

    public TransferRunner(int threadId) {
      this.random = new Random(threadId + context.getClientSeed());
    }

    public void run(AtomicBoolean running) {
      long runStart = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(context.getRampTime());

      while (running.get()) {
        int numAccounts = context.getNumAccounts();
        List<Integer> ids = new ArrayList<>();
        ids.add(random.nextInt(numAccounts)); // fromId
        ids.add(random.nextInt(numAccounts)); // toId
        int amount = random.nextInt(100) + 1;

        try {
          long start = System.currentTimeMillis();
          transfer(ids, amount);
          long end = System.currentTimeMillis();

          context.logLatency(runStart, start, end);
          context.logSuccess(this.currentTxId, ids, amount);
          continue;
        } catch (UnknownTransactionStatusException e1) {
          System.err.println(e1.getMessage());
          e1.getUnknownTransactionId().ifPresent(txId -> context.addUnknownTx(txId, ids));
        } catch (Exception e2) {
          System.err.println(e2.getMessage());
        }
        context.logFailure(this.currentTxId, ids, amount);
      }
    }

    private void transfer(List<Integer> ids, int amount)
        throws CrudException, CommitException, UnknownTransactionStatusException {
      DistributedTransaction transaction = manager.start();
      this.currentTxId = transaction.getId();

      int fromId = ids.get(0);
      int toId = ids.get(1);
      int fromType = 0;
      int toType = 0;
      if (fromId == toId) {
        toType = 1; // transfer between the same account
      }

      context.logStart(this.currentTxId, ids, amount);

      Get fromGet = prepareGet(fromId, fromType);
      Get toGet = prepareGet(toId, toType);

      Optional<Result> fromResult = transaction.get(fromGet);
      Optional<Result> toResult = transaction.get(toGet);
      IntValue fromBalance = add((IntValue) fromResult.get().getValue(BALANCE).get(), -amount);
      IntValue toBalance = add((IntValue) toResult.get().getValue(BALANCE).get(), amount);

      transaction.put(preparePut(fromId, fromType, fromBalance));
      transaction.put(preparePut(toId, toType, toBalance));
      transaction.commit();
    }

    private Get prepareGet(int id, int type) {
      Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
      Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
      return new Get(partitionKey, clusteringKey)
          .withConsistency(Consistency.LINEARIZABLE)
          .forNamespace(namespace)
          .forTable(table);
    }

    private Put preparePut(int id, int type, IntValue balance) {
      Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
      Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
      return new Put(partitionKey, clusteringKey)
          .withConsistency(Consistency.LINEARIZABLE)
          .forNamespace(namespace)
          .forTable(table)
          .withValue(balance);
    }

    private IntValue add(IntValue base, int amount) {
      return new IntValue(BALANCE, base.get() + amount);
    }
  }
}
