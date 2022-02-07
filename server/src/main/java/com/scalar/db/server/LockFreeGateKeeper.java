package com.scalar.db.server;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class LockFreeGateKeeper implements GateKeeper {

  private final AtomicBoolean isOpen;
  private final LongAdder outstandingRequestCount;

  public LockFreeGateKeeper() {
    isOpen = new AtomicBoolean(true);
    outstandingRequestCount = new LongAdder();
  }

  @Override
  public void open() {
    isOpen.set(true);
  }

  @Override
  public void close() {
    isOpen.set(false);
  }

  @Override
  public boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (outstandingRequestCount.longValue() > 0) {
      TimeUnit.MILLISECONDS.sleep(100);
      if (System.currentTimeMillis() - start >= unit.toMillis(timeout)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean letIn() {
    // To avoid the race condition, we need to increment "outstandingRequestCount" before checking
    // "isOpen"

    boolean incremented = false;

    // This is a preliminary check for "isOpen". We increment "outstandingRequestCount" only if the
    // check indicates it's an open state
    if (isOpen.get()) {
      outstandingRequestCount.increment();
      incremented = true;
    }

    // Check "isOpen". We must not increment "outstandingRequestCount" after this. Otherwise, the
    // race condition can happen
    if (!isOpen.get()) {
      // It's closed

      if (incremented) {
        // If we increment "outstandingRequestCount" and we reach here, it indicates that it was
        // closed between checking the first and second "isOpen" checks. In this case, we need to
        // decrement the count
        outstandingRequestCount.decrement();
      }
      return false;
    }

    // If we don't increment "outstandingRequestCount" and we reach here, it indicates that it was
    // opened between checking the first and second "isOpen" checks. In this case, we assume it's
    // still closed. Otherwise, it's an open state
    return incremented;
  }

  @Override
  public void letOut() {
    outstandingRequestCount.decrement();
  }
}
