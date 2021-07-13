package com.scalar.db.server;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class Pauser {
  private final AtomicBoolean isPaused;
  private final LongAdder outstandingRequestCount;

  public Pauser() {
    isPaused = new AtomicBoolean();
    outstandingRequestCount = new LongAdder();
  }

  public void pause() {
    isPaused.set(true);
  }

  public void unpause() {
    isPaused.set(false);
  }

  public boolean await(long timeout, TimeUnit unit) {
    long start = System.currentTimeMillis();
    while (outstandingRequestCount.longValue() > 0) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException ignored) {
      }
      if (System.currentTimeMillis() - start >= unit.toMillis(timeout)) {
        return false;
      }
    }
    return true;
  }

  /**
   * All of the gPRC endpoints that will be paused/unpaused need to call this method before
   * processing.
   *
   * @return when it's paused, returns false. Otherwise returns true
   */
  public boolean preProcess() {
    // To avoid the race condition, we need to increment "outstandingRequestCount" before
    // checking "isPaused"

    boolean incremented = false;

    // This is a preliminary check for "isPaused". We increment "outstandingRequestCount" only if
    // the check indicates it's not paused
    if (!isPaused.get()) {
      outstandingRequestCount.increment();
      incremented = true;
    }

    // Check "isPaused". We must not increment "outstandingRequestCount" after this. Otherwise, the
    // race condition can happen
    if (isPaused.get()) {
      // It's paused

      if (incremented) {
        // If we increment "outstandingRequestCount" and we reach here, it indicates that pause
        // happens between checking the first and second "isPaused" checks. In this case, we need
        // to decrement the count
        outstandingRequestCount.decrement();
      }
      return false;
    }

    // If we don't increment "outstandingRequestCount" and we reach here, it indicates that unpause
    // happens between checking the first and second "isPaused" checks. In this case, we assume
    // it's still paused. Otherwise, it's not paused
    return incremented;
  }

  /**
   * All of the gPRC endpoints that will be paused/unpaused need to call this method after
   * processing.
   */
  public void postProcess() {
    outstandingRequestCount.decrement();
  }

  @Override
  public String toString() {
    return "Pauser{"
        + "isPaused="
        + isPaused
        + ", outstandingRequestCount="
        + outstandingRequestCount
        + '}';
  }
}
