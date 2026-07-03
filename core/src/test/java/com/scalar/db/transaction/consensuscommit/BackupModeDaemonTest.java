package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
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
  public void scanAndUpdate_WithSingleBackingUpLabel_ShouldCacheThatLabel() throws Exception {
    // Arrange
    when(coordinator.scanBackingUpLabels()).thenReturn(Collections.singletonList("label-1"));

    // Act
    daemon.scanAndUpdate();

    // Assert
    assertThat(daemon.backingUpLabels()).containsExactly("label-1");
    assertThat(daemon.activeBackupLabel(Long.MAX_VALUE)).isEqualTo("label-1");
    verify(logger, never()).warn(any(String.class), any(Object.class));
  }

  @Test
  public void scanAndUpdate_WithNoBackingUpLabels_ShouldCacheEmptyAndReturnNullLabel()
      throws Exception {
    // Arrange
    when(coordinator.scanBackingUpLabels()).thenReturn(Collections.emptyList());

    // Act
    daemon.scanAndUpdate();

    // Assert
    assertThat(daemon.backingUpLabels()).isEmpty();
    assertThat(daemon.activeBackupLabel(Long.MAX_VALUE)).isNull();
  }

  @Test
  public void scanAndUpdate_WithMoreThanOneBackingUpLabel_ShouldLogWarningAndCacheAll()
      throws Exception {
    // Arrange
    when(coordinator.scanBackingUpLabels()).thenReturn(Arrays.asList("label-1", "label-2"));

    // Act
    daemon.scanAndUpdate();

    // Assert
    assertThat(daemon.backingUpLabels()).containsExactly("label-1", "label-2");
    verify(logger).warn(contains("single-window"), any(Object.class));
  }

  @Test
  public void scanAndUpdate_WhenScanFails_ShouldKeepPreviousLabelsAndNotAdvanceSuccessTime()
      throws Exception {
    // Arrange: a first successful scan at t=1000 caches label-1; the next scan fails at t=2000.
    when(coordinator.scanBackingUpLabels())
        .thenReturn(Collections.singletonList("label-1"))
        .thenThrow(new CoordinatorException("boom", new RuntimeException()));
    nowMillis = 1000L;
    daemon.scanAndUpdate();

    // Act
    nowMillis = 2000L;
    daemon.scanAndUpdate(); // Fails.

    // Assert: labels kept, and the last SUCCESSFUL read time is NOT advanced to the failed attempt.
    assertThat(daemon.backingUpLabels()).containsExactly("label-1");
    assertThat(daemon.lastSuccessfulReadAtMillis()).isEqualTo(1000L);
  }

  @Test
  public void activeBackupLabel_WhenNeverSuccessfullyScanned_ShouldReturnNullWithoutScanning()
      throws Exception {
    // Arrange: fresh daemon, no scan has ever succeeded (the backup table may not exist / CBRL
    // off).
    // Act
    String label = daemon.activeBackupLabel(1);

    // Assert: fail open (no window to miss), and do not even attempt a scan.
    assertThat(label).isNull();
    verify(coordinator, never()).scanBackingUpLabels();
  }

  @Test
  public void activeBackupLabel_WhenStaleAfterSuccessAndRescanFails_ShouldFailClosed()
      throws Exception {
    // Arrange: one successful scan at t=1000, then scans start failing; the cache goes stale.
    when(coordinator.scanBackingUpLabels())
        .thenReturn(Collections.singletonList("label-1"))
        .thenThrow(new CoordinatorException("unreachable", new RuntimeException()));
    nowMillis = 1000L;
    daemon.scanAndUpdate();
    nowMillis = 7000L; // 6000ms since the last success, past the 5000ms bound.

    // Act & Assert: it forces a re-scan (which fails) and then fails closed.
    assertThatThrownBy(() -> daemon.activeBackupLabel(5000))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failing closed");
    assertThat(daemon.lastSuccessfulReadAtMillis()).isEqualTo(1000L);
  }

  @Test
  public void activeBackupLabel_WhenStaleAfterSuccessAndRescanSucceeds_ShouldReturnFreshLabel()
      throws Exception {
    // Arrange: a successful scan at t=1000 (label-1), then the flag changes to label-2.
    when(coordinator.scanBackingUpLabels())
        .thenReturn(Collections.singletonList("label-1"))
        .thenReturn(Collections.singletonList("label-2"));
    nowMillis = 1000L;
    daemon.scanAndUpdate();
    nowMillis = 7000L; // Stale: forces a synchronous re-scan.

    // Act
    String label = daemon.activeBackupLabel(5000);

    // Assert: the forced re-scan refreshed the cache to the current label.
    assertThat(label).isEqualTo("label-2");
    assertThat(daemon.lastSuccessfulReadAtMillis()).isEqualTo(7000L);
    verify(coordinator, org.mockito.Mockito.times(2)).scanBackingUpLabels();
  }

  @Test
  public void activeBackupLabel_WhenStalenessBoundNonPositive_ShouldUseCacheWithoutFailingClosed()
      throws Exception {
    // Arrange: one successful scan, then a long time passes with no re-scan.
    when(coordinator.scanBackingUpLabels()).thenReturn(Collections.singletonList("label-1"));
    nowMillis = 1000L;
    daemon.scanAndUpdate();
    nowMillis = 1_000_000L; // Far past any positive bound.

    // Act: a non-positive bound disables staleness enforcement.
    String label = daemon.activeBackupLabel(0);

    // Assert: returns the cached label without failing closed or forcing a re-scan.
    assertThat(label).isEqualTo("label-1");
    verify(coordinator, org.mockito.Mockito.times(1)).scanBackingUpLabels();
  }
}
