package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GroupCommitter3Test {
  static class MyKeyManipulator implements KeyManipulator<String> {
    @Override
    public String createParentKey() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String createFullKey(String parentKey, String childKey) {
      return parentKey + ":" + childKey;
    }

    @Override
    public boolean isFullKey(String fullKey) {
      return fullKey.contains(":");
    }

    @Override
    public Keys<String> fromFullKey(String fullKey) {
      String[] parts = fullKey.split(":");
      return new Keys<>(parts[0], parts[1]);
    }
  }

  static class Value {
    public final String v;

    public Value(String v) {
      this.v = v;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("v", v).toString();
    }
  }

  static class KeyAndFuture {
    public final String key;

    public KeyAndFuture(String key, Future<Value> future) {
      this.key = key;
      this.future = future;
    }

    public final Future<Value> future;
  }

  static class Result {
    public final int tps;
    public final int retry;

    public Result(int tps, int retry) {
      this.tps = tps;
      this.retry = retry;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("tps", tps).add("retry", retry).toString();
    }
  }

  static class GroupCommitParams {
    public final int numOfRetentionValues;
    public final int sizeFixExpirationInMillis;
    public final int timeoutExpirationInMillis;

    public GroupCommitParams(
        int numOfRetentionValues, int sizeFixExpirationInMillis, int timeoutExpirationInMillis) {
      this.numOfRetentionValues = numOfRetentionValues;
      this.sizeFixExpirationInMillis = sizeFixExpirationInMillis;
      this.timeoutExpirationInMillis = timeoutExpirationInMillis;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("numOfRetentionValues", numOfRetentionValues)
          .add("sizeFixExpirationInMillis", sizeFixExpirationInMillis)
          .add("timeoutExpirationInMillis", timeoutExpirationInMillis)
          .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GroupCommitParams)) return false;
      GroupCommitParams that = (GroupCommitParams) o;
      return numOfRetentionValues == that.numOfRetentionValues
          && sizeFixExpirationInMillis == that.sizeFixExpirationInMillis
          && timeoutExpirationInMillis == that.timeoutExpirationInMillis;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          numOfRetentionValues, sizeFixExpirationInMillis, timeoutExpirationInMillis);
    }
  }

  // $ ./gradlew core:cleanTest core:test --tests
  // 'com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitter3Test'
  @Test
  void benchmark() throws ExecutionException, InterruptedException, TimeoutException {
    // Warmup
    benchmarkInternal(
        // For Benchmarker:
        // NumOfThreads,NumOfRequests
        256,
        40000,
        // AveragePrepareWaitInMillis,MultiplexerInMillis,MaxCommitWaitInMillis
        0,
        0,
        0,
        // For Group Commit
        new GroupCommitParams(8, 10, 100));

    System.out.println("FINISHED WARMUP");
    System.gc();
    System.out.println("STARTING BENCHMARK");

    boolean microBenchmark = false;
    if (microBenchmark) {
      // Benchmark for Micro-benchmark
      benchmarkInternal(
          // For Benchmarker:
          2048, // NumOfThreads
          100000, // NumOfRequests
          0, // AveragePrepareWaitInMillis
          0, // MultiplexerInMillis
          0, // MaxCommitWaitInMillis
          // For Group Commit
          new GroupCommitParams(40, 20, 200));
    } else {
      List<GroupCommitParams> params = Arrays.asList(new GroupCommitParams(40, 40, 400));
      Map<GroupCommitParams, Result> results = new HashMap<>();
      for (GroupCommitParams param : params) {
        // Benchmark for Production case
        Result result =
            benchmarkInternal(
                // For Benchmarker:
                2048, // NumOfThreads
                100000, // NumOfRequests
                100, // AveragePrepareWaitInMillis
                400, // MultiplexerInMillis
                100, // MaxCommitWaitInMillis
                // For Group Commit
                param);
        results.put(param, result);
        System.gc();
        System.out.println("FINISH: " + param);
        TimeUnit.SECONDS.sleep(10);
      }
      System.out.println("RESULT: " + results);
    }
  }

  Result benchmarkInternal(
      int bmNumOfThreads,
      int bmNumOfRequests,
      int bmAveragePrepareWaitInMillis,
      int bmMultiplexerInMillis,
      int bmMaxCommitWaitInMillis,
      GroupCommitParams groupCommitParams)
      throws ExecutionException, InterruptedException, TimeoutException {
    Random rand = new Random();
    AtomicInteger retry = new AtomicInteger();
    Map<String, Boolean> emittedKeys = new ConcurrentHashMap<>();

    try (GroupCommitter3<String, Value> groupCommitter =
        new GroupCommitter3<>(
            "test",
            groupCommitParams.sizeFixExpirationInMillis,
            groupCommitParams.timeoutExpirationInMillis,
            groupCommitParams.numOfRetentionValues,
            20,
            new MyKeyManipulator())) {
      groupCommitter.setEmitter(
          ((parentKey, values) -> {
            try {
              if (bmMaxCommitWaitInMillis > 0) {
                int waitInMillis = rand.nextInt(bmMaxCommitWaitInMillis);
                System.out.printf(
                    "Waiting for commit. ParentKey=%s, Duration=%d ms \n", parentKey, waitInMillis);
                TimeUnit.MILLISECONDS.sleep(waitInMillis);
              }
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            for (Value v : values) {
              if (emittedKeys.put(v.v, true) != null) {
                throw new RuntimeException(v + " is already set");
              }
            }
          }));

      List<KeyAndFuture> futures = new ArrayList<>();
      /*
      ScheduledExecutorService monitor =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder().setDaemon(true).build());
      monitor.scheduleAtFixedRate(
          () -> System.err.println("future.size:" + futures.size()), 1, 1, TimeUnit.SECONDS);
       */

      ExecutorService executorService =
          Executors.newFixedThreadPool(
              bmNumOfThreads, new ThreadFactoryBuilder().setDaemon(true).build());
      try {
        long start = System.currentTimeMillis();
        for (int i = 0; i < bmNumOfRequests; i++) {
          String childKey = String.format("%016d", i);
          Value value = new Value("ORIG-KEY: " + childKey);
          futures.add(
              new KeyAndFuture(
                  childKey,
                  executorService.submit(
                      () -> {
                        while (true) {
                          try {
                            String fullKey = groupCommitter.reserve(childKey);
                            int waitInMillis =
                                (int)
                                    (bmAveragePrepareWaitInMillis
                                        + rand.nextGaussian() * bmMultiplexerInMillis);
                            waitInMillis = Math.max(waitInMillis, bmAveragePrepareWaitInMillis);
                            if (waitInMillis > 0) {
                              System.out.printf(
                                  "Waiting for prepare. FullKey=%s, Duration=%d ms \n",
                                  fullKey, waitInMillis);
                              TimeUnit.MILLISECONDS.sleep(waitInMillis);
                            }
                            groupCommitter.ready(fullKey, value);
                            break;
                          } catch (GroupCommitAlreadyClosedException
                              | GroupCommitAlreadySizeFixedException e) {
                            retry.incrementAndGet();
                          }
                        }
                        return null;
                      })));
        }

        for (KeyAndFuture kf : futures) {
          try {
            System.err.println("Getting the future of " + kf.key);
            kf.future.get(10, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            System.out.println("Timeout: Key=" + kf.key);
            throw e;
          }
        }
        long duration = System.currentTimeMillis() - start;
        int tps = (int) ((double) bmNumOfRequests / (duration / 1000.0));
        System.err.println("Duration(ms): " + duration);
        System.err.println("TPS:          " + tps);
        System.err.println("Retry:        " + retry.get());

        start = System.currentTimeMillis();
        for (int i = 0; i < bmNumOfRequests; i++) {
          String expectedKey = "ORIG-KEY: " + String.format("%016d", i);
          if (!emittedKeys.containsKey(expectedKey)) {
            throw new AssertionError(expectedKey + " is not found");
          }
          // System.err.println("Confirmed the key is contained: Key=" + expectedKey);
        }
        assertEquals(bmNumOfRequests, emittedKeys.size());

        System.err.println("Checked all the keys");
        System.err.println("Duration(ms): " + (System.currentTimeMillis() - start));
        return new Result(tps, retry.get());
      } finally {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
      }
    }
  }
}
