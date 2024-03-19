package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GroupCommitConfigTest {
  @Test
  void slotCapacity_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.slotCapacity()).isEqualTo(10);
  }

  @Test
  void groupCloseTimeoutMillis_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.groupCloseTimeoutMillis()).isEqualTo(20);
  }

  @Test
  void delayedSlotMoveTimeoutMillis_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.delayedSlotMoveTimeoutMillis()).isEqualTo(30);
  }

  @Test
  void timeoutCheckIntervalMillis_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.timeoutCheckIntervalMillis()).isEqualTo(40);
  }

  @Test
  void metricsConsoleReporterEnabled_GivenTrue_ShouldReturnTrue() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, true);

    // Act / Assert
    assertThat(config.metricsConsoleReporterEnabled()).isTrue();
  }

  @Test
  void metricsConsoleReporterEnabled_GivenFalse_ShouldReturnFalse() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, false);

    // Act / Assert
    assertThat(config.metricsConsoleReporterEnabled()).isFalse();
  }

  @Test
  void metricsConsoleReporterEnabled_GivenNothing_ShouldReturnFalse() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.metricsConsoleReporterEnabled()).isFalse();
  }
}
