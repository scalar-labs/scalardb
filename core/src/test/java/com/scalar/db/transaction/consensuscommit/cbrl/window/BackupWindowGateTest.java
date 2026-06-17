package com.scalar.db.transaction.consensuscommit.cbrl.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class BackupWindowGateTest {

  private static final long NOW = 1_000L;

  private static BackupWindowGate gate(WindowSource source) {
    return new BackupWindowGate(source, () -> NOW);
  }

  private static BackupWindow window(long expiresAtMillis) {
    return new BackupWindow("w1", 1L, expiresAtMillis);
  }

  @Test
  void openWindow_notExpired_reportsOpen() {
    assertThat(gate(() -> Optional.of(window(NOW + 1))).observe()).isEqualTo(WindowState.OPEN);
  }

  @Test
  void noWindow_reportsClosed() {
    assertThat(gate(() -> Optional.<BackupWindow>empty()).observe()).isEqualTo(WindowState.CLOSED);
  }

  @Test
  void expiredWindow_atTtlBoundary_reportsClosed() {
    // expiresAt == now: a window is expired at the boundary (isExpired uses >=).
    assertThat(gate(() -> Optional.of(window(NOW))).observe()).isEqualTo(WindowState.CLOSED);
  }

  @Test
  void expiredWindow_pastTtl_reportsClosed() {
    assertThat(gate(() -> Optional.of(window(NOW - 1))).observe()).isEqualTo(WindowState.CLOSED);
  }

  @Test
  void unreadableSource_reportsUnconfirmable_failClosed() {
    assertThat(
            gate(() -> {
                  throw new RuntimeException("storage down");
                })
                .observe())
        .isEqualTo(WindowState.UNCONFIRMABLE);
  }
}
