package com.scalar.db.transaction.consensuscommit;

/**
 * Lifecycle states of a CBRL backup row (see the {@code backup} coordinator table). A backup row's
 * {@code state} column walks this small state machine, and each transition is a conditional {@code
 * putIf} on the current state. An app process logs redo <b>only</b> while a label's state is {@link
 * #BACKING_UP}; {@link #BACKUP_CANCELED} and {@link #RESTORE_CANCELED} are terminal.
 */
enum BackupState {
  BACKING_UP,
  BACKED_UP,
  BACKUP_CANCELED,
  RESTORING,
  RESTORED,
  RESTORE_CANCELED
}
