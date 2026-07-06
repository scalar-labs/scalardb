package com.scalar.db.transaction.consensuscommit;

/**
 * Lifecycle states of a CBRL backup row (see the {@code backup} coordinator table). A backup row's
 * {@code state} column walks this small state machine, and each transition is a conditional {@code
 * putIf} on the current state. An app process logs redo <b>only</b> while a label's state is {@link
 * #BACKING_UP}. {@link #BACKED_UP} (window closed after the coordinator snapshot) and {@link
 * #CANCELED} (window abandoned, or neutralized on a restored cluster so its daemon does not
 * re-enter backup mode) are terminal.
 */
enum BackupState {
  BACKING_UP,
  BACKED_UP,
  CANCELED
}
