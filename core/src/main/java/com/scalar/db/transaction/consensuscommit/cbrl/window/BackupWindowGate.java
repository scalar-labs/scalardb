package com.scalar.db.transaction.consensuscommit.cbrl.window;

import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Observed by the commit path to decide whether to log the full redo write set. Fails closed: if
 * the window state cannot be confirmed, it reports {@link WindowState#UNCONFIRMABLE} so the caller
 * can refuse to commit in a chain-breaking way rather than silently committing unlogged inside an
 * open window (PoC plan concern #2). A window past its TTL is reported {@link WindowState#CLOSED}.
 */
public final class BackupWindowGate {
  private final WindowSource source;
  private final LongSupplier clock;

  public BackupWindowGate(WindowSource source) {
    this(source, System::currentTimeMillis);
  }

  BackupWindowGate(WindowSource source, LongSupplier clock) {
    this.source = source;
    this.clock = clock;
  }

  public WindowState observe() {
    Optional<BackupWindow> window;
    try {
      window = source.currentWindow();
    } catch (Exception e) {
      // Fail closed: we could not confirm the window, so the caller must not assume it is closed.
      return WindowState.UNCONFIRMABLE;
    }
    if (!window.isPresent()) {
      return WindowState.CLOSED;
    }
    return window.get().isExpired(clock.getAsLong()) ? WindowState.CLOSED : WindowState.OPEN;
  }
}
