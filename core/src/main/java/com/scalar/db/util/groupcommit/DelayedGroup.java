package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

// A group for a delayed slot. This group contains only a single slot.
@ThreadSafe
class DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
    extends Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> {
  private final FULL_KEY fullKey;

  DelayedGroup(
      GroupCommitConfig config,
      FULL_KEY fullKey,
      Emittable<EMIT_PARENT_KEY, EMIT_FULL_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY>
          keyManipulator) {
    super(emitter, keyManipulator, 1, config.oldGroupAbortTimeoutMillis());
    this.fullKey = fullKey;
  }

  FULL_KEY fullKey() {
    return fullKey;
  }

  @Override
  FULL_KEY fullKey(CHILD_KEY childKey) {
    return fullKey;
  }

  @Override
  // Probably, this `synchronized` keyword could be removed since this class contains only a single
  // slot. But just in case.
  protected synchronized void delegateEmitTaskToWaiter() {
    assert slots.size() == 1;
    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot :
        slots.values()) {
      // Pass `emitter` to ask the receiver's thread to emit the value
      slot.delegateTaskToWaiter(
          () ->
              emitter.emitDelayedGroup(
                  keyManipulator.emitFullKeyFromFullKey(fullKey), slot.value()));
      // Return since the number of the slots is only 1.
      return;
    }
  }

  @Nullable
  @Override
  protected synchronized FULL_KEY reserveNewSlot(
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot) {
    slot.changeParentGroupToDelayedGroup(this);
    FULL_KEY fullKey = super.reserveNewSlot(slot);
    if (fullKey == null) {
      throw new AssertionError(
          String.format("This Group is unexpectedly size-fixed. Group:%s, Slot:%s", this, slot));
    }
    return fullKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DelayedGroup)) return false;
    DelayedGroup<?, ?, ?, ?, ?, ?> that = (DelayedGroup<?, ?, ?, ?, ?, ?>) o;
    return Objects.equal(fullKey, that.fullKey);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fullKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fullKey", fullKey)
        .add("status", status)
        .add("valueSlots.size", slots.size())
        .toString();
  }
}
