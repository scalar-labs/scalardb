package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import com.scalar.db.util.ThrowableSupplier;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

// A container of value which is stored in a group.
@ThreadSafe
class Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V, R> {
  private final AtomicReference<
          Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V, R>>
      parentGroup = new AtomicReference<>();
  private final CHILD_KEY key;
  // If the completed task is null, the group was already emitted by another slot's thread, and the
  // emit result is handed to this slot via `emitResult`. Otherwise, the task must be emitted by the
  // receiver's thread, which obtains the result as the task's return value.
  private final CompletableFuture<ThrowableSupplier<R, Exception>> completableFuture =
      new CompletableFuture<>();
  // The emit result handed to this slot when the group is emitted by another slot's thread.
  private final AtomicReference<R> emitResult = new AtomicReference<>();
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
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V, R>
          parentGroup) {
    this.key = key;
    this.parentGroup.set(parentGroup);
  }

  // This is called only once when being moved from NormalGroup to DelayedGroup.
  void changeParentGroupToDelayedGroup(
      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V, R>
          parentGroup) {
    this.parentGroup.set(parentGroup);
  }

  FULL_KEY fullKey() {
    return parentGroup.get().fullKey(key);
  }

  void setValue(V value) {
    this.value.set(Objects.requireNonNull(value));
  }

  @Nullable
  R waitUntilEmit() throws GroupCommitException {
    try {
      // If the task is null, the group was already emitted by another slot's thread and the result
      // was handed to this slot via `emitResult`. Otherwise, this slot's thread must run the emit
      // task and obtains the result as its return value.
      ThrowableSupplier<R, Exception> taskToEmit = completableFuture.get();
      if (taskToEmit != null) {
        return taskToEmit.get();
      }
      return emitResult.get();
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

  // Marks this slot as a success, handing it the emit result obtained by the emitter slot's thread.
  void markAsSuccess(R result) {
    // The order matters: emitResult must be set before completing the future. The waiter reads
    // emitResult.get() only after completableFuture.get() returns, and that completion is the
    // happens-before publication point that makes the result visible. Do not reorder these.
    emitResult.set(result);
    completableFuture.complete(null);
  }

  // Marks this slot as a failure.
  void markAsFailed(Exception e) {
    completableFuture.completeExceptionally(e);
  }

  // Delegates the emit task to the client. The client receiving this task needs to handle the emit
  // task and obtains the emit result as its return value.
  void delegateTaskToWaiter(ThrowableSupplier<R, Exception> task) {
    completableFuture.complete(task);
  }

  boolean isReady() {
    return value.get() != null;
  }

  boolean isDone() {
    return isDone.get();
  }
}
