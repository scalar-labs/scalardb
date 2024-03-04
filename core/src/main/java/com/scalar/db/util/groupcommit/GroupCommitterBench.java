package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** A tentative benchmarker. This will be removed later. */
class GroupCommitterBench {
  private static class ExpectedException extends RuntimeException {
    public ExpectedException(String message) {
      super(message);
    }
  }

  private static class MyKeyManipulator implements KeyManipulator<String, String, String, String> {
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
      List<String> parts = Splitter.on(':').splitToList(fullKey);
      return new Keys<>(parts.get(0), parts.get(1), fullKey);
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

  private static class Value {
    public final String v;

    public Value(String v) {
      this.v = v;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("v", v).toString();
    }
  }

  private static class KeyAndFuture {
    public final String key;

    public KeyAndFuture(String key, Future<Void> future) {
      this.key = key;
      this.future = future;
    }

    public final Future<Void> future;
  }

  private static class Result {
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

  void benchmark(boolean microBenchmark)
      throws ExecutionException, InterruptedException, TimeoutException {
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
        0,
        0,
        new GroupCommitConfig(8, 10, 100, 20));
    System.out.println("FINISHED WARMUP");
    System.gc();
    System.out.println("STARTING BENCHMARK");

    if (microBenchmark) {
      // Benchmark for Micro-benchmark
      benchmarkInternal(
          // For Benchmarker:
          2048, // NumOfThreads
          800000, // NumOfRequests
          0, // AveragePrepareWaitInMillis
          0, // MultiplexerInMillis
          0, // MaxCommitWaitInMillis
          1, // ErrorBeforeReadyPercentage
          1, // ErrorAfterReadyPercentage
          // For Group Commit
          new GroupCommitConfig(40, 20, 200, 20));
    } else {
      List<GroupCommitConfig> configs = Arrays.asList(new GroupCommitConfig(40, 40, 200, 20));
      Map<GroupCommitConfig, Result> results = new HashMap<>();
      for (GroupCommitConfig config : configs) {
        // Benchmark for Production case
        Result result =
            benchmarkInternal(
                // For Benchmarker:
                2048, // NumOfThreads
                200000, // NumOfRequests
                200, // AveragePrepareWaitInMillis
                400, // MultiplexerInMillis
                100, // MaxCommitWaitInMillis
                1, // ErrorBeforeReadyPercentage
                1, // ErrorAfterReadyPercentage
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
      int numOfThreads,
      int numOfRequests,
      int averageDurationBeforeReadyInMillis,
      int multiplexerInMillis,
      int maxEmitDurationInMillis,
      int errorBeforeReadyPercentage,
      int errorAfterReadyPercentage,
      GroupCommitConfig groupCommitConfig)
      throws ExecutionException, InterruptedException, TimeoutException {
    Random rand = new Random();
    AtomicInteger retry = new AtomicInteger();
    Map<String, Boolean> emittedKeys = new ConcurrentHashMap<>();
    Map<String, Boolean> failedKeys = new ConcurrentHashMap<>();
    Emittable<String, Value> emitter =
        (parentKey, values) -> {
          // This lambda is executed once the group is ready to group-commit.
          try {
            if (maxEmitDurationInMillis > 0) {
              int waitInMillis = rand.nextInt(maxEmitDurationInMillis);
              TimeUnit.MILLISECONDS.sleep(waitInMillis);
            }
            if (errorAfterReadyPercentage > rand.nextInt(100)) {
              for (Value v : values) {
                // Remember the value as a failure.
                if (failedKeys.put(v.v, true) != null) {
                  throw new RuntimeException(v + " is already set");
                }
              }
              throw new ExpectedException("Error after READY");
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          for (Value v : values) {
            // Remember the value as a success.
            if (emittedKeys.put(v.v, true) != null) {
              throw new RuntimeException(v + " is already set");
            }
          }
        };

    try (GroupCommitter<String, String, String, String, Value> groupCommitter =
        new GroupCommitter<>("test", groupCommitConfig, new MyKeyManipulator())) {
      groupCommitter.setEmitter(emitter);

      List<KeyAndFuture> futures = new ArrayList<>();

      ExecutorService executorService =
          Executors.newFixedThreadPool(
              numOfThreads, new ThreadFactoryBuilder().setDaemon(true).build());
      try {
        long start = System.currentTimeMillis();
        for (int i = 0; i < numOfRequests; i++) {
          String childKey = String.format("%016d", i);
          Value value = new Value("ORIG-KEY: " + childKey);
          Callable<Void> groupCommitCaller =
              () -> {
                String fullKey = null;
                try {
                  // Reserve a slot to put a value later.
                  fullKey = groupCommitter.reserve(childKey);

                  // Wait for a random duration following Gaussian (or normal) distribution.
                  int waitInMillis =
                      (int)
                          (averageDurationBeforeReadyInMillis
                              + rand.nextGaussian() * multiplexerInMillis);
                  waitInMillis = Math.max(waitInMillis, averageDurationBeforeReadyInMillis);
                  if (waitInMillis > 0) {
                    TimeUnit.MILLISECONDS.sleep(waitInMillis);
                  }

                  // Fail at a certain rate.
                  if (errorBeforeReadyPercentage > rand.nextInt(100)) {
                    if (failedKeys.put(value.v, true) != null) {
                      throw new RuntimeException(value.v + " is already set");
                    }
                    throw new ExpectedException("Error before READY");
                  }

                  // Put the value in the slot and wait until the group is emitted.
                  groupCommitter.ready(fullKey, value);
                } catch (Exception e) {
                  if (fullKey != null) {
                    // This is needed since GroupCommitter can't remove the garbage when
                    // an exception is thrown before `ready()`.
                    groupCommitter.remove(fullKey);
                  }
                  throw e;
                }
                return null;
              };

          futures.add(new KeyAndFuture(childKey, executorService.submit(groupCommitCaller)));
        }

        // Check the futures.
        checkFutures(futures);

        // Print the summary.
        int tps = printSummary(numOfRequests, start, retry.get());

        // Check the results.
        checkResults(numOfRequests, emittedKeys, failedKeys);

        // To see garbage groups remain.
        checkGarbage(groupCommitter);

        return new Result(tps, retry.get());
      } finally {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
      }
    }
  }

  private void checkFutures(List<KeyAndFuture> futures)
      throws ExecutionException, TimeoutException, InterruptedException {
    for (KeyAndFuture kf : futures) {
      try {
        System.err.println("Getting the future of " + kf.key);
        kf.future.get(10, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ExpectedException
            || (e.getCause() instanceof GroupCommitException
                && e.getCause().getCause() instanceof ExpectedException)) {
          // Expected ???
        } else {
          throw e;
        }
      } catch (ExpectedException e) {
        // Expected.
      } catch (TimeoutException e) {
        System.out.println("Timeout: Key=" + kf.key);
        throw e;
      }
    }
  }

  private int printSummary(long numOfRequests, long startInMillis, int retry) {
    long duration = System.currentTimeMillis() - startInMillis;
    int tps = (int) ((double) numOfRequests / (duration / 1000.0));
    System.err.println("Duration(ms): " + duration);
    System.err.println("TPS:          " + tps);
    System.err.println("Retry:        " + retry);
    return tps;
  }

  private void checkResults(
      long numOfRequests, Map<String, Boolean> emittedKeys, Map<String, Boolean> failedKeys) {
    for (int i = 0; i < numOfRequests; i++) {
      String expectedKey = key(i);
      if (!emittedKeys.containsKey(expectedKey) && !failedKeys.containsKey(expectedKey)) {
        throw new AssertionError(expectedKey + " is not found");
      }
      // System.err.println("Confirmed the key is contained: Key=" + expectedKey);
    }
    System.err.println("Checked all the keys");
  }

  private void checkGarbage(GroupCommitter<String, String, String, String, Value> groupCommitter)
      throws InterruptedException {
    boolean noGarbage = false;
    Metrics metrics = null;
    for (int i = 0; i < 10; i++) {
      metrics = groupCommitter.getMetrics();
      if (metrics.sizeOfNormalGroupMap == 0
          && metrics.sizeOfDelayedGroupMap == 0
          && metrics.sizeOfQueueForClosingNormalGroup == 0
          && metrics.sizeOfQueueForMovingDelayedSlot == 0
          && metrics.sizeOfQueueForCleaningUpGroup == 0) {
        System.out.println("No garbage remains in GroupCommitter.");
        noGarbage = true;
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    if (!noGarbage) {
      System.out.println("Some garbage remains in GroupCommitter. " + metrics);
    }
  }

  private String key(int i) {
    return "ORIG-KEY: " + String.format("%016d", i);
  }

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, TimeoutException {
    boolean microBenchmark =
        Boolean.parseBoolean(Optional.ofNullable(System.getenv("MICRO_BENCHMARK")).orElse("false"));
    System.out.println("microBenchmark: " + microBenchmark);
    new GroupCommitterBench().benchmark(microBenchmark);
  }
}
