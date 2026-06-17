package com.scalar.db.transaction.consensuscommit.cbrl.window;

/**
 * An open CBRL backup window: the durable record that says "redo logging (and logical-delete mode)
 * is on." Carries an {@code epoch} so a stale cache can be detected and an {@code expiresAtMillis}
 * TTL so a crashed backup process cannot pin the window open forever.
 */
public final class BackupWindow {
  private final String windowId;
  private final long epoch;
  private final long expiresAtMillis;

  public BackupWindow(String windowId, long epoch, long expiresAtMillis) {
    this.windowId = windowId;
    this.epoch = epoch;
    this.expiresAtMillis = expiresAtMillis;
  }

  public String windowId() {
    return windowId;
  }

  public long epoch() {
    return epoch;
  }

  public long expiresAtMillis() {
    return expiresAtMillis;
  }

  /** A window past its TTL is treated as closed even if it was never explicitly closed. */
  public boolean isExpired(long nowMillis) {
    return nowMillis >= expiresAtMillis;
  }

  @Override
  public String toString() {
    return "BackupWindow{id="
        + windowId
        + ", epoch="
        + epoch
        + ", expiresAt="
        + expiresAtMillis
        + '}';
  }
}
