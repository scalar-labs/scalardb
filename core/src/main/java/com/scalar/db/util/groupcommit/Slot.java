package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import com.scalar.db.util.ThrowableRunnable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

// A container of value which is stored in a group.
@ThreadSafe
class Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> {
  private final AtomicReference<
          Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>>
      parentGroup = new AtomicReference<>();
  private final CHILD_KEY key;
  // If a result value is null, the value is already emitted.
  // Otherwise, the result lambda must be emitted by the receiver's thread.
  private final CompletableFuture<ThrowableRunnable<Exception>> completableFuture =
      new CompletableFuture<>();
  // This value can be changed from null -> non-null, not vice versa.
  private final AtomicReference<V> value = new AtomicReference<>();
  // The status of Slot becomes done once the client obtains the result not when a value is set.
  // In NormalGroup, any client could potentially be delayed to obtain the result. The state of a
  // group should not move to State.DONE until all the clients get the result on their slots.
  //
  // This value changes only from false to true.
  private final AtomicBoolean isDone = new AtomicBoolean();

  Slot(
      CHILD_KEY key,
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> parentGroup) {
    this.key = key;
    this.parentGroup.set(parentGroup);
  }

  // This is called only once when being moved from NormalGroup to DelayedGroup.
  void changeParentGroupToDelayedGroup(
      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
          parentGroup) {
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
      // Otherwise, the result lambda must be emitted by the waiter's thread.
      ThrowableRunnable<Exception> taskToEmit = completableFuture.get();
      if (taskToEmit != null) {
        taskToEmit.run();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new GroupCommitException(
          String.format("Group commit was interrupted, Group: %s", parentGroup.get()), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof GroupCommitException) {
        throw (GroupCommitException) cause;
      }
      throw new GroupCommitException(
          String.format("Group commit failed. Group: %s", parentGroup.get()), cause);
    } catch (Exception e) {
      if (e instanceof GroupCommitException) {
        throw (GroupCommitException) e;
      }
      throw new GroupCommitException(
          String.format("Group commit failed. Group: %s", parentGroup.get()), e);
    } finally {
      // Slot gets done once the client obtains the result.
      isDone.set(true);
      parentGroup.get().updateStatus();
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
  void markAsFailed(Exception e) {
    completableFuture.completeExceptionally(e);
  }

  // Delegates the emit task to the client. The client receiving this task needs to handle the emit
  // task.
  void delegateTaskToWaiter(ThrowableRunnable<Exception> task) {
    completableFuture.complete(task);
  }

  boolean isReady() {
    return value.get() != null;
  }

  boolean isDone() {
    return isDone.get();
  }
}
