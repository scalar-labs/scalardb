package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

class Slot<K, V> {
  private final NormalGroup<K, V> parentGroup;
  private final K key;
  // If a result value is null, the value is already emitted.
  // Otherwise, the result lambda must be emitted by the receiver's thread.
  private final CompletableFuture<ThrowableRunnable> completableFuture = new CompletableFuture<>();
  // TODO: Revisit this
  // This value can be changed from null -> non-null.
  @Nullable private volatile V value;

  Slot(K key, NormalGroup<K, V> parentGroup) {
    this.key = key;
    this.parentGroup = parentGroup;
  }

  K getFullKey() {
    return parentGroup.getFullKey(key);
  }

  void putValue(V value) {
    this.value = Objects.requireNonNull(value);
  }

  void waitUntilEmit() throws GroupCommitException {
    try {
      // If a result value is null, the value is already emitted.
      // Otherwise, the result lambda must be emitted by the receiver's thread.
      ThrowableRunnable taskToEmit = completableFuture.get();
      if (taskToEmit != null) {
        // TODO: Enhance the error handling?
        taskToEmit.run();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new GroupCommitException("Group commit was interrupted", e);
    } catch (Exception e) {
      if (e instanceof GroupCommitException) {
        throw (GroupCommitException) e;
      }
      throw new GroupCommitException("Group commit failed", e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key).toString();
  }

  K getKey() {
    return key;
  }

  @Nullable
  V getValue() {
    return value;
  }

  void success() {
    completableFuture.complete(null);
  }

  void delegateTask(ThrowableRunnable task) {
    completableFuture.complete(task);
  }

  void fail(Exception e) {
    completableFuture.completeExceptionally(e);
  }
}
