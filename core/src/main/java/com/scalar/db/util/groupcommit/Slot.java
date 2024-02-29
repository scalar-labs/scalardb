package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

// A container of value which is stored in a group.
class Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private final Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> parentGroup;
  private final CHILD_KEY key;
  // If a result value is null, the value is already emitted.
  // Otherwise, the result lambda must be emitted by the receiver's thread.
  private final CompletableFuture<ThrowableRunnable> completableFuture = new CompletableFuture<>();
  // This value can be changed from null -> non-null, not vice versa.
  @Nullable private V value;

  Slot(CHILD_KEY key, Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> parentGroup) {
    this.key = key;
    this.parentGroup = parentGroup;
  }

  FULL_KEY fullKey() {
    return parentGroup.fullKey(key);
  }

  void setValue(V value) {
    this.value = Objects.requireNonNull(value);
  }

  void waitUntilEmit() throws GroupCommitException {
    try {
      // If a result value is null, the value is already emitted.
      // Otherwise, the result lambda must be emitted by the receiver's thread.
      ThrowableRunnable taskToEmit = completableFuture.get();
      if (taskToEmit != null) {
        taskToEmit.run();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new GroupCommitException("Group commit was interrupted", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof GroupCommitException) {
        throw (GroupCommitException) cause;
      }
      throw new GroupCommitException("Group commit failed", cause);
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

  CHILD_KEY key() {
    return key;
  }

  @Nullable
  V value() {
    return value;
  }

  void markAsSuccess() {
    completableFuture.complete(null);
  }

  void markAsFail(Exception e) {
    completableFuture.completeExceptionally(e);
  }

  void delegateTaskToWaiter(ThrowableRunnable task) {
    completableFuture.complete(task);
  }
}
