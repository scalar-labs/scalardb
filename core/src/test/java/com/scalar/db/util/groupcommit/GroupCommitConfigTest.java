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
}
