package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GroupCommitConfigTest {
  @Test
  void slotCapacity_GivenArbitraryValue_ShouldReturnIt() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.slotCapacity()).isEqualTo(10);
  }

  @Test
  void groupCloseTimeoutMillis_GivenArbitraryValue_ShouldReturnIt() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.groupCloseTimeoutMillis()).isEqualTo(20);
  }

  @Test
  void delayedSlotMoveTimeoutMillis_GivenArbitraryValue_ShouldReturnIt() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.delayedSlotMoveTimeoutMillis()).isEqualTo(30);
  }

  @Test
  void timeoutCheckIntervalMillis_GivenArbitraryValue_ShouldReturnIt() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(10, 20, 30, 40);

    // Act / Assert
    assertThat(config.timeoutCheckIntervalMillis()).isEqualTo(40);
  }
}
