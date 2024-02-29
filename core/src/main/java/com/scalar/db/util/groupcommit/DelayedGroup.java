package com.scalar.db.util.groupcommit;

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
      GarbageDelayedGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          garbageGroupCollector) {
    super(emitter, keyManipulator, 1);
    this.fullKey = fullKey;
    this.garbageGroupCollector = garbageGroupCollector;
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
    assert slots.size() == 1;
    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
      // Pass `emitter` to ask the receiver's thread to emit the value
      slot.delegateTaskToWaiter(
          () -> {
            try {
              emitter.execute(
                  keyManipulator.emitKeyFromFullKey(fullKey),
                  Collections.singletonList(slot.value()));
            } finally {
              dismiss();
            }
          });
      // Return since the number of the slots is only 1.
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
