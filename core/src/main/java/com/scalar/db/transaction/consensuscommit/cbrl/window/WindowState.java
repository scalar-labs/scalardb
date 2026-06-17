package com.scalar.db.transaction.consensuscommit.cbrl.window;

/**
 * What the commit path sees when it observes the backup window. {@link #UNCONFIRMABLE} is the
 * fail-closed signal: the node could not confirm the current state, so it must not commit in a way
 * that could leave a gap in the redo log.
 */
public enum WindowState {
  OPEN,
  CLOSED,
  UNCONFIRMABLE
}
