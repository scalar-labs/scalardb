package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.Test;

class GroupCommitterConcurrentTest {
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

  @Test
  void testConcurrentlyWithoutWait()
      throws ExecutionException, InterruptedException, TimeoutException {
    new Runner(
            2048, // NumOfThreads
            800000, // NumOfRequests
            0, // AveragePrepareWaitInMillis
            0, // MultiplexerInMillis
            0, // MaxCommitWaitInMillis
            1, // ErrorBeforeReadyPercentage
            1 // ErrorAfterReadyPercentage
            )
        .exec(new GroupCommitConfig(40, 20, 200, 20));
  }

  @Test
  void testConcurrentlyWithWait()
      throws ExecutionException, InterruptedException, TimeoutException {
    new Runner(
            2048, // NumOfThreads
            200000, // NumOfRequests
            200, // AveragePrepareWaitInMillis
            400, // MultiplexerInMillis
            100, // MaxCommitWaitInMillis
            1, // ErrorBeforeReadyPercentage
            1 // ErrorAfterReadyPercentage
            )
        .exec(new GroupCommitConfig(40, 40, 200, 20));
  }

  private static class Runner {
    private final int numOfThreads;
    private final int numOfRequests;
    private final int averageDurationBeforeReadyInMillis;
    private final int multiplexerInMillis;
    private final int maxEmitDurationInMillis;
    private final int errorBeforeReadyPercentage;
    private final int errorAfterReadyPercentage;
    private final Map<String, Boolean> emittedKeys = new ConcurrentHashMap<>();
    private final Map<String, Boolean> failedKeys = new ConcurrentHashMap<>();
    private final Random rand = new Random();
    private final AtomicInteger retry = new AtomicInteger();

    Runner(
        int numOfThreads,
        int numOfRequests,
        int averageDurationBeforeReadyInMillis,
        int multiplexerInMillis,
        int maxEmitDurationInMillis,
        int errorBeforeReadyPercentage,
        int errorAfterReadyPercentage) {
      this.numOfThreads = numOfThreads;
      this.numOfRequests = numOfRequests;
      this.averageDurationBeforeReadyInMillis = averageDurationBeforeReadyInMillis;
      this.multiplexerInMillis = multiplexerInMillis;
      this.maxEmitDurationInMillis = maxEmitDurationInMillis;
      this.errorBeforeReadyPercentage = errorBeforeReadyPercentage;
      this.errorAfterReadyPercentage = errorAfterReadyPercentage;
    }

    // Returns a lambda that will be executed once the group is ready to group-commit.
    private Emittable<String, Value> emitter() {
      return (parentKey, values) -> {
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
    }

    private Callable<Void> groupCommitterCaller(
        GroupCommitter<String, String, String, String, Value> groupCommitter,
        String childKey,
        Value value) {
      return () -> {
        String fullKey = null;
        try {
          // Reserve a slot to put a value later.
          fullKey = groupCommitter.reserve(childKey);

          // Wait for a random duration following Gaussian (or normal) distribution.
          int waitInMillis =
              (int)
                  (averageDurationBeforeReadyInMillis + rand.nextGaussian() * multiplexerInMillis);
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
    }

    private void exec(GroupCommitConfig groupCommitConfig)
        throws ExecutionException, InterruptedException, TimeoutException {
      try (GroupCommitter<String, String, String, String, Value> groupCommitter =
          new GroupCommitter<>("test", groupCommitConfig, new MyKeyManipulator())) {
        groupCommitter.setEmitter(emitter());

        List<KeyAndFuture> futures = new ArrayList<>();

        ExecutorService executorService =
            Executors.newFixedThreadPool(
                numOfThreads, new ThreadFactoryBuilder().setDaemon(true).build());
        try {
          long startInMillis = System.currentTimeMillis();
          for (int i = 0; i < numOfRequests; i++) {
            String childKey = String.format("%016d", i);
            Value value = new Value("ORIG-KEY: " + childKey);
            Callable<Void> groupCommitCaller =
                groupCommitterCaller(groupCommitter, childKey, value);
            futures.add(new KeyAndFuture(childKey, executorService.submit(groupCommitCaller)));
          }

          // Check the futures.
          checkFutures(futures);

          // Print the summary.
          int tps = printSummary(startInMillis);

          // Check the results.
          checkResults();

          // To see garbage groups remain.
          checkGarbage(groupCommitter);
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

    private int printSummary(long startInMillis) {
      long duration = System.currentTimeMillis() - startInMillis;
      int tps = (int) ((double) numOfRequests / (duration / 1000.0));
      System.err.println("Duration(ms): " + duration);
      System.err.println("TPS:          " + tps);
      System.err.println("Retry:        " + retry.get());
      return tps;
    }

    private void checkResults() {
      for (int i = 0; i < numOfRequests; i++) {
        String expectedKey = key(i);
        if (!emittedKeys.containsKey(expectedKey) && !failedKeys.containsKey(expectedKey)) {
          throw new AssertionError(expectedKey + " is not found");
        }
      }
    }

    private void checkGarbage(GroupCommitter<String, String, String, String, Value> groupCommitter)
        throws InterruptedException {
      boolean noGarbage = false;
      Metrics metrics = null;
      for (int i = 0; i < 10; i++) {
        metrics = groupCommitter.getMetrics();
        if (metrics.sizeOfNormalGroupMap == 0
            && metrics.sizeOfDelayedGroupMap == 0
            && metrics.queueLengthOfGroupCloseWorker == 0
            && metrics.queueLengthOfDelayedSlotMoveWorker == 0
            && metrics.queueLengthOfGroupCleanupWorker == 0) {
          noGarbage = true;
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }
      if (!noGarbage) {
        throw new AssertionError("Some garbage remains in GroupCommitter. " + metrics);
      }
    }

    private String key(int i) {
      return "ORIG-KEY: " + String.format("%016d", i);
    }
  }
}
