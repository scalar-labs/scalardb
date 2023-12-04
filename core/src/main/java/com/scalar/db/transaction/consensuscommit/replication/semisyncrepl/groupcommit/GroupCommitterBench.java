package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
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

class GroupCommitterBench {
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

  // $ ./gradlew core:cleanTest core:test --tests
  // 'com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitter3Test'
  void benchmark() throws ExecutionException, InterruptedException, TimeoutException {
    // Warmup
    benchmarkInternal(
        // For Benchmarker:
        // NumOfThreads,NumOfRequests
        256,
        4000,
        // AveragePrepareWaitInMillis,MultiplexerInMillis,MaxCommitWaitInMillis
        0,
        0,
        0,
        // For Group Commit
        // NumOfRetentionValues,SizeFixExpirationInMillis,TimeoutExpirationInMillis,NumOfThreads
        2,
        10,
        100,
        64);

    boolean microBenchmark = false;
    if (microBenchmark) {
      // Benchmark for Micro-benchmark
      benchmarkInternal(
          // For Benchmarker:
          32, // NumOfThreads
          100000, // NumOfRequests
          0, // AveragePrepareWaitInMillis
          0, // MultiplexerInMillis
          0, // MaxCommitWaitInMillis
          // For Group Commit
          32, // NumOfRetentionValues
          80, // SizeFixExpirationInMillis
          800, // TimeoutExpirationInMillis
          64 // NumOfThreads
          );
    } else {
      // Benchmark for Production case
      benchmarkInternal(
          // For Benchmarker:
          4096, // NumOfThreads
          100000, // NumOfRequests
          40, // AveragePrepareWaitInMillis
          400, // MultiplexerInMillis
          40, // MaxCommitWaitInMillis
          // For Group Commit
          32, // NumOfRetentionValues
          50, // SizeFixExpirationInMillis
          200, // TimeoutExpirationInMillis
          64 // NumOfThreads
          );
    }
  }

  void benchmarkInternal(
      int bmNumOfThreads,
      int bmNumOfRequests,
      int bmAveragePrepareWaitInMillis,
      int bmMultiplexerInMillis,
      int bmMaxCommitWaitInMillis,
      int gcNumOfRetentionValues,
      int gcSizeFixExpirationInMillis,
      int gcTimeoutExpirationInMillis,
      int gcNumOfThreads)
      throws ExecutionException, InterruptedException, TimeoutException {
    Random rand = new Random();
    AtomicInteger retry = new AtomicInteger();
    Map<String, Boolean> emittedKeys = new ConcurrentHashMap<>();

    try (GroupCommitter3<String, Value> groupCommitter =
        new GroupCommitter3<>(
            "test",
            gcSizeFixExpirationInMillis,
            gcTimeoutExpirationInMillis,
            gcNumOfRetentionValues,
            5,
            gcNumOfThreads,
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
      System.err.println("Duration(ms): " + duration);
      System.err.println("TPS:          " + (((double) bmNumOfRequests) / (duration / 1000.0)));
      System.err.println("Retry:        " + retry.get());

      start = System.currentTimeMillis();
      for (int i = 0; i < bmNumOfRequests; i++) {
        String expectedKey = "ORIG-KEY: " + String.format("%016d", i);
        if (!emittedKeys.containsKey(expectedKey)) {
          throw new AssertionError(expectedKey + " is not found");
        }
        // System.err.println("Confirmed the key is contained: Key=" + expectedKey);
      }

      if (bmNumOfRequests != emittedKeys.size()) {
        throw new AssertionError(
            String.format(
                "bmNumOfRequests:%d, emittedKeys.size():%d", bmNumOfRequests, emittedKeys.size()));
      }

      System.err.println("Checked all the keys");
      System.err.println("Duration(ms): " + (System.currentTimeMillis() - start));
    }
  }

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, TimeoutException {
    new GroupCommitterBench().benchmark();
  }
}
