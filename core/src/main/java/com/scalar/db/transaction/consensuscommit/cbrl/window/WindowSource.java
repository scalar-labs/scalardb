package com.scalar.db.transaction.consensuscommit.cbrl.window;

import java.util.Optional;

/**
 * The durable source of truth for the backup window (e.g. a coordinator-side table). May fail to
 * answer (storage error, timeout, partition); the gate treats any failure as fail-closed.
 */
public interface WindowSource {
  /**
   * The currently-open window, or empty if no window is open.
   *
   * @throws Exception if the current state cannot be confirmed — the gate maps this to {@link
   *     WindowState#UNCONFIRMABLE}.
   */
  Optional<BackupWindow> currentWindow() throws Exception;
}
