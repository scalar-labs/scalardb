package com.scalar.db.util.groupcommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import java.util.Collections;

// A group for a delayed slot. This group contains only a single slot.
class DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private final FULL_KEY fullKey;
  private final GarbageDelayedGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      garbageGroupCollector;

  DelayedGroup(
      FULL_KEY fullKey,
      Emittable<EMIT_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator,
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot,
      GarbageDelayedGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          garbageGroupCollector) {
    super(emitter, keyManipulator, 1);
    this.fullKey = fullKey;
    this.garbageGroupCollector = garbageGroupCollector;

    // TODO: Revisit here
    // Auto emit should be disabled since:
    // - the queue and worker for delayed values will emit this if it's ready
    // - to avoid taking time in synchronized blocks
    checkNotNull(reserveNewSlot(slot, false));
  }

  FULL_KEY fullKey() {
    return fullKey;
  }

  @Override
  FULL_KEY fullKey(CHILD_KEY childKey) {
    return fullKey;
  }

  @Override
  protected void asyncEmit() {
    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
      // Pass `emitter` to ask the receiver's thread to emit the value
      slot.delegateTaskToWaiter(
          () ->
              emitter.execute(
                  keyManipulator.emitKeyFromFullKey(fullKey),
                  Collections.singletonList(slot.value())));
      // The number of the slots is only 1.
      dismiss();
      return;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DelayedGroup)) return false;
    DelayedGroup<?, ?, ?, ?, ?> that = (DelayedGroup<?, ?, ?, ?, ?>) o;
    return Objects.equal(fullKey, that.fullKey);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fullKey);
  }

  @Override
  protected void dismiss() {
    garbageGroupCollector.collect(this);
  }
}
