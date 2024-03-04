package com.scalar.db.util.groupcommit;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

class GroupCommitterTest {
  static class MyKeyManipulator implements KeyManipulator<String, String, String, String> {
    @Override
    public String generateParentKey() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String fullKey(String parentKey, String childKey) {
      return parentKey + ":" + childKey;
    }

    @Override
    public boolean isFullKey(Object obj) {
      if (!(obj instanceof String)) {
        return false;
      }
      String key = (String) obj;
      return key.contains(":");
    }

    @Override
    public Keys<String, String, String> keysFromFullKey(String fullKey) {
      String[] parts = fullKey.split(":");
      return new Keys<>(parts[0], parts[1], fullKey);
    }

    @Override
    public String emitKeyFromFullKey(String s) {
      return s;
    }

    @Override
    public String emitKeyFromParentKey(String s) {
      return s;
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
        new GroupCommitConfig(8, 10, 100, 20));

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
          new GroupCommitConfig(40, 20, 200, 20));
    } else {
      List<GroupCommitConfig> configs = Arrays.asList(new GroupCommitConfig(40, 40, 400, 20));
      Map<GroupCommitConfig, Result> results = new HashMap<>();
      for (GroupCommitConfig config : configs) {
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
                config);
        results.put(config, result);
        System.gc();
        System.out.println("FINISH: " + config);
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
      GroupCommitConfig groupCommitConfig)
      throws ExecutionException, InterruptedException, TimeoutException {
    Random rand = new Random();
    AtomicInteger retry = new AtomicInteger();
    Map<String, Boolean> emittedKeys = new ConcurrentHashMap<>();

    try (GroupCommitter<String, String, String, String, Value> groupCommitter =
        new GroupCommitter<>("test", groupCommitConfig, new MyKeyManipulator())) {
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
