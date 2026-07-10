package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

public class BackupModeDaemonTest {

  @Mock private Coordinator coordinator;
  @Mock private Logger logger;

  // A controllable clock so staleness/fail-closed is deterministic (no real sleeps).
  private long nowMillis = 1000L;
  private final LongSupplier clock = () -> nowMillis;

  private BackupModeDaemon daemon;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    daemon = new BackupModeDaemon(coordinator, 5000, logger, clock);
  }

  @Test
  public void readAndUpdate_WithOpenWindow_ShouldCacheThatLabel() throws Exception {
    // Arrange
    when(coordinator.getBackupLabel()).thenReturn(Optional.of("label-1"));

    // Act
    daemon.readAndUpdate();

    // Assert
    assertThat(daemon.activeLabel()).isEqualTo("label-1");
    assertThat(daemon.activeBackupLabel(Long.MAX_VALUE)).isEqualTo("label-1");
  }

  @Test
  public void readAndUpdate_WithNoOpenWindow_ShouldCacheNullLabel() throws Exception {
    // Arrange
    when(coordinator.getBackupLabel()).thenReturn(Optional.empty());

    // Act
    daemon.readAndUpdate();

    // Assert
    assertThat(daemon.activeLabel()).isNull();
    assertThat(daemon.activeBackupLabel(Long.MAX_VALUE)).isNull();
  }

  @Test
  public void readAndUpdate_WhenReadFails_ShouldKeepPreviousLabelAndNotAdvanceSuccessTime()
      throws Exception {
    // Arrange: a first successful read at t=1000 caches label-1; the next read fails at t=2000.
    when(coordinator.getBackupLabel())
        .thenReturn(Optional.of("label-1"))
        .thenThrow(new CoordinatorException("boom", new RuntimeException()));
    nowMillis = 1000L;
    daemon.readAndUpdate();

    // Act
    nowMillis = 2000L;
    daemon.readAndUpdate(); // Fails.

    // Assert: label kept, and the last SUCCESSFUL read time is NOT advanced to the failed attempt.
    assertThat(daemon.activeLabel()).isEqualTo("label-1");
    assertThat(daemon.lastSuccessfulReadAtMillis()).isEqualTo(1000L);
  }

  @Test
  public void activeBackupLabel_WhenNeverSuccessfullyRead_ShouldReturnNullWithoutReading()
      throws Exception {
    // Arrange: fresh daemon, no read has ever succeeded (the backup table may not exist / CBRL
    // off).
    // Act
    String label = daemon.activeBackupLabel(1);

    // Assert: fail open (no window to miss), and do not even attempt a read.
    assertThat(label).isNull();
    verify(coordinator, never()).getBackupLabel();
  }

  @Test
  public void activeBackupLabel_WhenStaleAfterSuccessAndRereadFails_ShouldFailClosed()
      throws Exception {
    // Arrange: one successful read at t=1000, then reads start failing; the cache goes stale.
    when(coordinator.getBackupLabel())
        .thenReturn(Optional.of("label-1"))
        .thenThrow(new CoordinatorException("unreachable", new RuntimeException()));
    nowMillis = 1000L;
    daemon.readAndUpdate();
    nowMillis = 7000L; // 6000ms since the last success, past the 5000ms bound.

    // Act & Assert: it forces a re-read (which fails) and then fails closed.
    assertThatThrownBy(() -> daemon.activeBackupLabel(5000))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failing closed");
    assertThat(daemon.lastSuccessfulReadAtMillis()).isEqualTo(1000L);
  }

  @Test
  public void activeBackupLabel_WhenStaleAfterSuccessAndRereadSucceeds_ShouldReturnFreshLabel()
      throws Exception {
    // Arrange: a successful read at t=1000 (label-1), then the flag changes to label-2.
    when(coordinator.getBackupLabel())
        .thenReturn(Optional.of("label-1"))
        .thenReturn(Optional.of("label-2"));
    nowMillis = 1000L;
    daemon.readAndUpdate();
    nowMillis = 7000L; // Stale: forces a synchronous re-read.

    // Act
    String label = daemon.activeBackupLabel(5000);

    // Assert: the forced re-read refreshed the cache to the current label.
    assertThat(label).isEqualTo("label-2");
    assertThat(daemon.lastSuccessfulReadAtMillis()).isEqualTo(7000L);
    verify(coordinator, org.mockito.Mockito.times(2)).getBackupLabel();
  }

  @Test
  public void activeBackupLabel_WhenStalenessBoundNonPositive_ShouldUseCacheWithoutFailingClosed()
      throws Exception {
    // Arrange: one successful read, then a long time passes with no re-read.
    when(coordinator.getBackupLabel()).thenReturn(Optional.of("label-1"));
    nowMillis = 1000L;
    daemon.readAndUpdate();
    nowMillis = 1_000_000L; // Far past any positive bound.

    // Act: a non-positive bound disables staleness enforcement.
    String label = daemon.activeBackupLabel(0);

    // Assert: returns the cached label without failing closed or forcing a re-read.
    assertThat(label).isEqualTo("label-1");
    verify(coordinator, org.mockito.Mockito.times(1)).getBackupLabel();
  }
}
