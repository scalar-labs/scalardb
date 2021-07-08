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
   * @return whether or not it's paused or not
   */
  public boolean preProcess() {
    boolean incremented = false;
    if (!isPaused.get()) {
      outstandingRequestCount.increment();
      incremented = true;
    }
    if (isPaused.get()) {
      if (incremented) {
        outstandingRequestCount.decrement();
      }
      return true;
    }
    return !incremented;
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
