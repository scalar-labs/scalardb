package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryQueue<T> {
  private final BlockingQueue<T> queue;

  public InMemoryQueue(int queueSize) {
    queue = new LinkedBlockingQueue<>();
  }

  @Override
  public String toString() {
    ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("queue.length", queue.size());
    return helper.toString();
  }

  public void enqueue(int partitionId, T value) {
    queue.add(value);
  }

  public T dequeue(int partitionId) throws InterruptedException {
    return queue.take();
  }

  public int size() {
    return queue.size();
  }
}
