package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

// A container of value which is stored in a group.
class Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private final AtomicReference<Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> parentGroup =
      new AtomicReference<>();
  private final CHILD_KEY key;
  // If a result value is null, the value is already emitted.
  // Otherwise, the result lambda must be emitted by the receiver's thread.
  private final CompletableFuture<ThrowableRunnable> completableFuture = new CompletableFuture<>();
  // This value can be changed from null -> non-null, not vice versa.
  private final AtomicReference<V> value = new AtomicReference<>();
  // The status of Slot becomes done once the client obtains the result not when a value is set.
  // In NormalGroup, any client could potentially be delayed to obtain the result. A group should
  // not move to State.DONE until all the clients get the result on their slots.
  private final AtomicBoolean isDone = new AtomicBoolean();

  Slot(CHILD_KEY key, NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> parentGroup) {
    this.key = key;
    this.parentGroup.set(parentGroup);
  }

  void changeParentGroupToDelayedGroup(
      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> parentGroup) {
    this.parentGroup.set(parentGroup);
  }

  FULL_KEY fullKey() {
    return parentGroup.get().fullKey(key);
  }

  void setValue(V value) {
    this.value.set(Objects.requireNonNull(value));
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
    } finally {
      // Slot gets done once the client obtains the result.
      isDone.set(true);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("isReady", isReady())
        .add("isDone", isDone())
        .toString();
  }

  CHILD_KEY key() {
    return key;
  }

  @Nullable
  V value() {
    return value.get();
  }

  // Marks this slot as a success.
  void markAsSuccess() {
    completableFuture.complete(null);
  }

  // Marks this slot as a failure.
  void markAsFail(Exception e) {
    completableFuture.completeExceptionally(e);
  }

  // Delegates the emit task to the client. The client receiving this task needs to handle the emit
  // task.
  void delegateTaskToWaiter(ThrowableRunnable task) {
    completableFuture.complete(task);
  }

  boolean isReady() {
    return value.get() != null;
  }

  boolean isDone() {
    return isDone.get();
  }
}
