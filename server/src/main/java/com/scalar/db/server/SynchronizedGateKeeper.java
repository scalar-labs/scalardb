package com.scalar.db.server;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SynchronizedGateKeeper implements GateKeeper {
  private boolean isOpen;
  private int numOutstandingRequests;

  public SynchronizedGateKeeper() {
    isOpen = true;
    numOutstandingRequests = 0;
  }

  @Override
  public synchronized void open() {
    isOpen = true;
  }

  @Override
  public synchronized void close() {
    isOpen = false;
  }

  @Override
  public synchronized boolean awaitDrained(long timeout, TimeUnit unit)
      throws InterruptedException {
    long timeoutNanos = unit.toNanos(timeout);
    long endTimeNanos = System.nanoTime() + timeoutNanos;
    while (numOutstandingRequests > 0 && (timeoutNanos = endTimeNanos - System.nanoTime()) > 0) {
      NANOSECONDS.timedWait(this, timeoutNanos);
    }
    return numOutstandingRequests == 0;
  }

  @Override
  public synchronized boolean letIn() {
    if (isOpen) {
      ++numOutstandingRequests;
      return true;
    }
    return false;
  }

  @Override
  public synchronized void letOut() {
    if (--numOutstandingRequests == 0) {
      notifyAll();
    }
  }
}
