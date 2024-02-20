package com.scalar.db.util.groupcommit;

import java.util.Collections;
import java.util.Map.Entry;

class DelayedGroup<K, V> extends Group<K, V> {
  DelayedGroup(
      K fullKey,
      Emittable<K, V> emitter,
      KeyManipulator<K> keyManipulator,
      long sizeFixExpirationInMillis,
      long timeoutExpirationInMillis,
      Slot<K, V> slot,
      GarbageGroupCollector<K, V> garbageGroupCollector) {
    super(
        fullKey,
        emitter,
        keyManipulator,
        sizeFixExpirationInMillis,
        timeoutExpirationInMillis,
        1,
        garbageGroupCollector);
    try {
      // Auto emit should be disabled since:
      // - the queue and worker for delayed values will emit this if it's ready
      // - to avoid taking time in synchronized blocks
      super.reserveNewSlot(slot, false);
    } catch (GroupCommitAlreadyClosedException e) {
      // FIXME: Message
      throw new IllegalStateException(
          "Failed to reserve a value slot. This shouldn't happen. slot:" + slot, e);
    }
  }

  @Override
  protected void asyncEmit() {
    for (Entry<K, Slot<K, V>> entry : slots.entrySet()) {
      Slot<K, V> slot = entry.getValue();
      // Pass `emitter` to ask the receiver's thread to emit the value
      slot.delegateTask(() -> emitter.execute(key, Collections.singletonList(slot.getValue())));
      // The number of the slots is only 1.
      dismiss();
      return;
    }
  }
}
