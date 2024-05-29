package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.Emittable;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CoordinatorGroupCommitterTest {
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  @Mock private Emittable<String, String, Snapshot> emitter;
  @Captor private ArgumentCaptor<List<Snapshot>> snapshotsArgumentCaptor;
  @Captor private ArgumentCaptor<Snapshot> snapshotArgumentCaptor;

  @Test
  void reserve_GivenArbitraryChildTxId_ShouldReturnFullTxId() throws Exception {
    try (CoordinatorGroupCommitter groupCommitter =
        new CoordinatorGroupCommitter(new GroupCommitConfig(2, 100, 400, 60000, 10))) {
      groupCommitter.setEmitter(emitter);
      // Arrange
      String childTxId1 = UUID.randomUUID().toString();
      String childTxId2 = UUID.randomUUID().toString();
      String childTxId3 = UUID.randomUUID().toString();
      String childTxId4 = UUID.randomUUID().toString();

      // Act
      String fullTxId1 = groupCommitter.reserve(childTxId1);
      String fullTxId2 = groupCommitter.reserve(childTxId2);
      String fullTxId3 = groupCommitter.reserve(childTxId3);
      String fullTxId4 = groupCommitter.reserve(childTxId4);

      // Assert
      assertThat(keyManipulator.isFullKey(fullTxId1)).isTrue();
      assertThat(keyManipulator.isFullKey(fullTxId2)).isTrue();
      assertThat(keyManipulator.isFullKey(fullTxId3)).isTrue();
      assertThat(keyManipulator.isFullKey(fullTxId4)).isTrue();
      assertThat(keyManipulator.keysFromFullKey(fullTxId1).childKey).isEqualTo(childTxId1);
      assertThat(keyManipulator.keysFromFullKey(fullTxId2).childKey).isEqualTo(childTxId2);
      assertThat(keyManipulator.keysFromFullKey(fullTxId3).childKey).isEqualTo(childTxId3);
      assertThat(keyManipulator.keysFromFullKey(fullTxId4).childKey).isEqualTo(childTxId4);
      assertThat(keyManipulator.keysFromFullKey(fullTxId1).parentKey)
          .isEqualTo(keyManipulator.keysFromFullKey(fullTxId2).parentKey);
      assertThat(keyManipulator.keysFromFullKey(fullTxId3).parentKey)
          .isEqualTo(keyManipulator.keysFromFullKey(fullTxId4).parentKey);
      assertThat(keyManipulator.keysFromFullKey(fullTxId1).parentKey)
          .isNotEqualTo(keyManipulator.keysFromFullKey(fullTxId3).parentKey);
      verify(emitter, never()).emitNormalGroup(any(), any());
      verify(emitter, never()).emitDelayedGroup(any(), any());

      groupCommitter.remove(fullTxId1);
      groupCommitter.remove(fullTxId2);
      groupCommitter.remove(fullTxId3);
      groupCommitter.remove(fullTxId4);
    }
  }

  @Test
  void ready_GivenArbitrarySnapshot_ShouldWaitUntilGroupCommitted() throws Exception {
    try (CoordinatorGroupCommitter groupCommitter =
        new CoordinatorGroupCommitter(new GroupCommitConfig(2, 1000, 1000, 60000, 10))) {
      groupCommitter.setEmitter(emitter);
      // Arrange
      String childTxId1 = UUID.randomUUID().toString();
      String childTxId2 = UUID.randomUUID().toString();
      String childTxId3 = UUID.randomUUID().toString();
      String childTxId4 = UUID.randomUUID().toString();

      String fullTxId1 = groupCommitter.reserve(childTxId1);
      String fullTxId2 = groupCommitter.reserve(childTxId2);
      String fullTxId3 = groupCommitter.reserve(childTxId3);
      String fullTxId4 = groupCommitter.reserve(childTxId4);

      Snapshot snapshot1 = mock(Snapshot.class);
      Snapshot snapshot2 = mock(Snapshot.class);
      Snapshot snapshot3 = mock(Snapshot.class);
      Snapshot snapshot4 = mock(Snapshot.class);

      // Act
      ExecutorService executorService = Executors.newCachedThreadPool();
      List<Future<Void>> futures = new ArrayList<>();
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId1, snapshot1);
                return null;
              }));
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId2, snapshot2);
                return null;
              }));
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId3, snapshot3);
                return null;
              }));
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId4, snapshot4);
                return null;
              }));
      executorService.shutdown();

      // Assert
      for (Future<Void> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      verify(emitter)
          .emitNormalGroup(
              eq(keyManipulator.keysFromFullKey(fullTxId1).parentKey),
              snapshotsArgumentCaptor.capture());
      assertThat(snapshotsArgumentCaptor.getValue()).containsOnly(snapshot1, snapshot2);
      verify(emitter)
          .emitNormalGroup(
              eq(keyManipulator.keysFromFullKey(fullTxId3).parentKey),
              snapshotsArgumentCaptor.capture());
      assertThat(snapshotsArgumentCaptor.getValue()).containsOnly(snapshot3, snapshot4);
      verify(emitter, never()).emitDelayedGroup(any(), any());
    }
  }

  @Test
  void ready_GivenArbitrarySnapshotWithSomeDelay_ShouldWaitUntilSeparatelyGroupCommitted()
      throws Exception {
    try (CoordinatorGroupCommitter groupCommitter =
        new CoordinatorGroupCommitter(new GroupCommitConfig(2, 500, 500, 60000, 10))) {
      groupCommitter.setEmitter(emitter);
      // Arrange
      String childTxId1 = UUID.randomUUID().toString();
      String childTxId2 = UUID.randomUUID().toString();

      String fullTxId1 = groupCommitter.reserve(childTxId1);
      String fullTxId2 = groupCommitter.reserve(childTxId2);

      Snapshot snapshot1 = mock(Snapshot.class);
      Snapshot snapshot2 = mock(Snapshot.class);

      // Act
      ExecutorService executorService = Executors.newCachedThreadPool();
      List<Future<Void>> futures = new ArrayList<>();
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId1, snapshot1);
                return null;
              }));
      // Sleep to trigger some timeouts in the group commit.
      TimeUnit.MILLISECONDS.sleep(1000);
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId2, snapshot2);
                return null;
              }));
      executorService.shutdown();

      // Assert
      for (Future<Void> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      verify(emitter)
          .emitNormalGroup(
              eq(keyManipulator.keysFromFullKey(fullTxId1).parentKey),
              snapshotsArgumentCaptor.capture());
      assertThat(snapshotsArgumentCaptor.getValue()).containsOnly(snapshot1);
      verify(emitter).emitDelayedGroup(eq(fullTxId2), snapshotArgumentCaptor.capture());
      assertThat(snapshotArgumentCaptor.getValue()).isEqualTo(snapshot2);
    }
  }

  @Test
  void remove_GivenOneOfFullTxIds_ShouldRemoveItAndProceedTheOther() throws Exception {
    try (CoordinatorGroupCommitter groupCommitter =
        new CoordinatorGroupCommitter(new GroupCommitConfig(2, 4000, 4000, 60000, 10))) {
      groupCommitter.setEmitter(emitter);
      // Arrange
      String childTxId1 = UUID.randomUUID().toString();
      String childTxId2 = UUID.randomUUID().toString();

      String fullTxId1 = groupCommitter.reserve(childTxId1);
      String fullTxId2 = groupCommitter.reserve(childTxId2);

      Snapshot snapshot = mock(Snapshot.class);

      ExecutorService executorService = Executors.newCachedThreadPool();
      List<Future<Void>> futures = new ArrayList<>();
      futures.add(
          executorService.submit(
              () -> {
                groupCommitter.ready(fullTxId2, snapshot);
                return null;
              }));
      executorService.shutdown();

      // Act
      groupCommitter.remove(fullTxId1);

      // Assert
      for (Future<Void> future : futures) {
        // Short timeout is enough since there is no delayed transaction.
        future.get(1, TimeUnit.SECONDS);
      }
      verify(emitter)
          .emitNormalGroup(
              eq(keyManipulator.keysFromFullKey(fullTxId2).parentKey),
              snapshotsArgumentCaptor.capture());
      assertThat(snapshotsArgumentCaptor.getValue()).containsOnly(snapshot);
      verify(emitter, never()).emitDelayedGroup(any(), any());
    }
  }

  @Test
  void remove_GivenAllFullTxIds_ShouldRemoveAll() throws Exception {
    try (CoordinatorGroupCommitter groupCommitter =
        new CoordinatorGroupCommitter(new GroupCommitConfig(2, 500, 500, 60000, 10))) {
      groupCommitter.setEmitter(emitter);
      // Arrange
      String childTxId1 = UUID.randomUUID().toString();
      String childTxId2 = UUID.randomUUID().toString();

      String fullTxId1 = groupCommitter.reserve(childTxId1);
      String fullTxId2 = groupCommitter.reserve(childTxId2);

      // Act
      groupCommitter.remove(fullTxId1);
      groupCommitter.remove(fullTxId2);
      TimeUnit.MILLISECONDS.sleep(1000);

      // Assert
      verify(emitter, never()).emitNormalGroup(any(), any());
      verify(emitter, never()).emitDelayedGroup(any(), any());
    }
  }

  @Nested
  static class CoordinatorGroupCommitKeyManipulatorTest {
    private final CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();

    @Test
    void isFullKey_GivenValidFullKey_ShouldReturnTrue() {
      // Arrange
      String childTxId = UUID.randomUUID().toString();

      // Act
      // Assert
      assertThat(keyManipulator.isFullKey("012345678901234567890123:" + childTxId)).isTrue();
      assertThat(keyManipulator.isFullKey("abcdefghijklmnopqrstuvwx:" + childTxId)).isTrue();
      assertThat(keyManipulator.isFullKey("cdefghijklmnopqrstuvwxyz:" + childTxId)).isTrue();
      assertThat(keyManipulator.isFullKey("ABCDEFGHIJKLMNOPQRSTUVWX:" + childTxId)).isTrue();
      assertThat(keyManipulator.isFullKey("CDEFGHIJKLMNOPQRSTUVWXYZ:" + childTxId)).isTrue();
      assertThat(keyManipulator.isFullKey("0123456789abcdefghijWXYZ:" + childTxId)).isTrue();
    }

    @Test
    void isFullKey_GivenInvalidFullKey_ShouldReturnFalse() {
      // Arrange
      String childTxId = UUID.randomUUID().toString();

      // Act
      // Assert
      assertThat(keyManipulator.isFullKey("01234567890123456789012:" + childTxId)).isFalse();
      assertThat(keyManipulator.isFullKey("0123456789012345678901234:" + childTxId)).isFalse();
      assertThat(keyManipulator.isFullKey("012345678901234567890123" + childTxId)).isFalse();
      assertThat(keyManipulator.isFullKey("0123456789012345678901234" + childTxId)).isFalse();
    }

    @Test
    void generateParentKey_ShouldReturnProperValue() {
      // Arrange
      int n = 10000;

      // Act
      // Assert
      for (int i = 0; i < n; i++) {
        String parentKey = keyManipulator.generateParentKey();
        assertThat(parentKey).hasSize(24);
        for (int j = 0; j < parentKey.length(); j++) {
          char c = parentKey.charAt(j);
          assertThat((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
              .isTrue();
        }
      }
    }

    @Test
    void generateParentKey_EvenWhenExecutingManyTime_ShouldNotReturnSameValue() {
      // Arrange
      int n = 10000;
      Set<String> parentKeys = new HashSet<>();

      // Act
      // Assert
      for (int i = 0; i < n; i++) {
        assertThat(parentKeys.add(keyManipulator.generateParentKey())).isTrue();
      }
      assertThat(parentKeys).hasSize(n);
    }

    @Test
    void fullKey_GivenArbitraryParentKeyAndChildKey_ShouldReturnProperValue() {
      // Arrange
      String parentKey = keyManipulator.generateParentKey();
      String childKey = UUID.randomUUID().toString();

      // Act
      // Assert
      assertThat(keyManipulator.fullKey(parentKey, childKey)).isEqualTo(parentKey + ":" + childKey);
    }
  }
}
