package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import static org.junit.jupiter.api.Assertions.*;

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
import java.util.concurrent.ScheduledExecutorService;
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

  // $ ./gradlew core:cleanTest core:test --tests
  // 'com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitter3Test'
  @Test
  void benchmark() throws ExecutionException, InterruptedException, TimeoutException {
    int numOfThreads = 512;
    int numOfRequests = 100000;
    int averagePrepareWaitInMillis = 40;
    int multiplexerInMillis = 300;
    int maxCommitWaitInMillis = 40;
    Random rand = new Random();
    AtomicInteger retry = new AtomicInteger();
    Map<String, Boolean> emittedKeys = new ConcurrentHashMap<>();

    GroupCommitter3<String, Value> groupCommitter =
        new GroupCommitter3<>("test", 10, 400, 32, 5, 64, new MyKeyManipulator());
    groupCommitter.setEmitter(
        ((parentKey, values) -> {
          try {
            TimeUnit.MILLISECONDS.sleep(rand.nextInt(maxCommitWaitInMillis));
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
    ScheduledExecutorService monitor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build());
    monitor.scheduleAtFixedRate(
        () -> {
          System.err.println("future.size:" + futures.size());
        },
        1,
        1,
        TimeUnit.SECONDS);

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            numOfThreads, new ThreadFactoryBuilder().setDaemon(true).build());
    long start = System.currentTimeMillis();
    for (int i = 0; i < numOfRequests; i++) {
      int finalI = i;
      String childKey = String.format("%016d", finalI);
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
                                (averagePrepareWaitInMillis
                                    + rand.nextGaussian() * multiplexerInMillis);
                        if (waitInMillis > 0) {
                          TimeUnit.MILLISECONDS.sleep(averagePrepareWaitInMillis);
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
    System.err.println("Duration(ms): " + (System.currentTimeMillis() - start));
    System.err.println("Retry: " + retry.get());

    start = System.currentTimeMillis();
    for (int i = 0; i < numOfRequests; i++) {
      String expectedKey = "ORIG-KEY: " + String.format("%016d", i);
      if (!emittedKeys.containsKey(expectedKey)) {
        throw new AssertionError(expectedKey + " is not found");
      }
      // System.err.println("Confirmed the key is contained: Key=" + expectedKey);
    }
    assertEquals(numOfRequests, emittedKeys.size());

    System.err.println("Checked all the keys");
    System.err.println("Duration(ms): " + (System.currentTimeMillis() - start));
  }
}
