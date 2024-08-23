package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryQueue<T> {
  private final List<BlockingQueue<T>> queues;

  public InMemoryQueue(int queueSize) {
    this.queues = new ArrayList<>(queueSize);
    for (int i = 0; i < queueSize; i++) {
      queues.add(new LinkedBlockingQueue<>());
    }
  }

  @Override
  public String toString() {
    ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (int i = 0; i < queues.size(); i++) {
      BlockingQueue<T> queue = queues.get(i);
      helper.add(String.format("queue-%d.length", i), queue.size());
    }
    return helper.toString();
  }

  public void enqueue(int partitionId, T value) {
    queues.get(Math.abs(partitionId) % queues.size()).add(value);
  }

  public T dequeue(int partitionId) throws InterruptedException {
    return queues.get(Math.abs(partitionId) % queues.size()).take();
  }

  public int size() {
    return queues.size();
  }
}
