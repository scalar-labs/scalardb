package com.scalar.db.transaction.consensuscommit;

/**
 * Terminal outcome of a CBRL backup window, recorded in the {@code backup_histories} coordinator
 * table when the window closes: {@link #BACKED_UP} (closed normally after the coordinator snapshot)
 * or {@link #CANCELED} (abandoned, or neutralized on a restored cluster so its daemon does not
 * re-enter backup mode). An open window is not a state here; it is the mere presence of the single
 * {@code backup} row.
 */
enum BackupState {
  BACKED_UP,
  CANCELED
}
