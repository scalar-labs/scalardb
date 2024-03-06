package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NormalGroupTest {
  private Emittable<String, Integer> emitter;
  private TestableKeyManipulator keyManipulator;

  @BeforeEach
  void setUp() {
    emitter = (s, values) -> {};
    keyManipulator = new TestableKeyManipulator();
  }

  @Test
  void parentKey_GivenKeyManipulator_ShouldReturnProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 4, new CurrentTime());

    // Act
    // Assert
    assertThat(group.parentKey()).isEqualTo("0000");
  }

  @Test
  void fullKey_GivenKeyManipulator_ShouldReturnProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 4, new CurrentTime());

    // Act
    // Assert
    assertThat(group.fullKey("child-key")).isEqualTo("0000:child-key");
  }

  @Test
  void reserveNewSlot() {}

  @Test
  void removeNotReadySlots() {}

  @Test
  void asyncEmit() {}

  @Test
  void groupClosedAt_GivenCurrentTime_ShouldReturnProperly() {
    // Arrange
    long startTimeMillis = System.currentTimeMillis();
    CurrentTime currentTime = spy(new CurrentTime());
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 4, currentTime);

    // Act
    // Assert
    assertThat(group.groupClosedMillisAt()).isEqualTo(startTimeMillis + 100);
  }

  @Test
  void delayedSlotMovedAt_GivenCurrentTime_ShouldReturnProperly() {
    // Arrange
    long startTimeMillis = System.currentTimeMillis();
    CurrentTime currentTime = spy(new CurrentTime());
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 4, currentTime);

    // Act
    // Assert
    assertThat(group.delayedSlotMovedMillisAt()).isEqualTo(startTimeMillis + 1000);
  }

  @Test
  void updateDelayedSlotMovedAt_GivenCurrentTime_ShouldUpdateProperly() {
    // Arrange
    long startTimeMillis = System.currentTimeMillis();
    CurrentTime currentTime = spy(new CurrentTime());
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 4, currentTime);

    // Act
    long updateTimeMillis = startTimeMillis + 10;
    doReturn(updateTimeMillis).when(currentTime).currentTimeMillis();
    group.updateDelayedSlotMovedAt();

    // Assert
    assertThat(group.delayedSlotMovedMillisAt()).isEqualTo(updateTimeMillis + 1000);
  }
}
