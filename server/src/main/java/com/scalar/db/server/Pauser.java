package com.scalar.db.server;

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

  /**
   * All of the gPRC endpoints that will be paused/unpaused need to call this method before
   * processing
   *
   * @return when it's paused, returns false. Otherwise returns trueq
   */
  public boolean preProcess() {
    boolean incremented = false;
    // We need to increment "outstandingRequestCount" before checking "isPaused" to avoid the race
    // condition
    if (!isPaused.get()) {
      outstandingRequestCount.increment();
      incremented = true;
    }
    if (isPaused.get()) {
      // It's paused
      if (incremented) {
        // We need to decrement "outstandingRequestCount" only when we incremented it
        outstandingRequestCount.decrement();
      }
      return false;
    }

    // If we didn't increment "outstandingRequestCount", we assume it's still paused, otherwise it's
    // not paused
    return incremented;
  }

  /**
   * All of the gPRC endpoints that will be paused/unpaused need to call this method after
   * processing
   */
  public void postProcess() {
    outstandingRequestCount.decrement();
  }

  public boolean outstandingRequestsExist() {
    return outstandingRequestCount.longValue() > 0;
  }

  public long getOutstandingRequestCount() {
    return outstandingRequestCount.longValue();
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
