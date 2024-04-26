package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GroupCommitConfigTest {
  @Test
  void slotCapacity_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50);

    // Act / Assert
    assertThat(config.slotCapacity()).isEqualTo(10);
  }

  @Test
  void groupCloseTimeoutMillis_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50);

    // Act / Assert
    assertThat(config.groupSizeFixTimeoutMillis()).isEqualTo(20);
  }

  @Test
  void delayedSlotMoveTimeoutMillis_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50);

    // Act / Assert
    assertThat(config.delayedSlotMoveTimeoutMillis()).isEqualTo(30);
  }

  @Test
  void oldGroupAbortTimeoutSeconds_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50);

    // Act / Assert
    assertThat(config.oldGroupAbortTimeoutSeconds()).isEqualTo(40);
  }

  @Test
  void timeoutCheckIntervalMillis_WithArbitraryValue_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50);

    // Act / Assert
    assertThat(config.timeoutCheckIntervalMillis()).isEqualTo(50);
  }

  @Test
  void metricsMonitorLogEnabled_GivenNoParameter_ShouldReturnFalseAsDefaultValue() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50);

    // Act / Assert
    assertThat(config.metricsMonitorLogEnabled()).isFalse();
  }

  @Test
  void metricsMonitorLogEnabled_GivenTrue_ShouldReturnTrue() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50, true);

    // Act / Assert
    assertThat(config.metricsMonitorLogEnabled()).isTrue();
  }

  @Test
  void metricsMonitorLogEnabled_GivenFalse_ShouldReturnFalse() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40, 50, false);

    // Act / Assert
    assertThat(config.metricsMonitorLogEnabled()).isFalse();
  }
}
