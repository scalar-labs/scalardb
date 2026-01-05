package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Get;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionContextTest {
  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE = "table";
  private static final String ANY_ID = "id";
  private static final String ANY_NAME = "name";
  private static final String ANY_TEXT = "text";

  @Mock private Snapshot snapshot;

  private Get prepareGet() {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE)
        .partitionKey(Key.ofText(ANY_NAME, ANY_TEXT))
        .build();
  }

  @Test
  public void isValidationRequired_WhenSnapshotIsolation_ShouldReturnFalse() {
    // Arrange
    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isValidationRequired_WhenReadCommittedIsolation_ShouldReturnFalse() {
    // Arrange
    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.READ_COMMITTED, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isValidationRequired_WhenSerializableIsolationWithNonEmptyScanSet_ShouldReturnTrue() {
    // Arrange
    when(snapshot.isScanSetEmpty()).thenReturn(false);
    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void
      isValidationRequired_WhenSerializableIsolationWithNonEmptyScannerSet_ShouldReturnTrue() {
    // Arrange
    when(snapshot.isScanSetEmpty()).thenReturn(true);
    when(snapshot.isScannerSetEmpty()).thenReturn(false);
    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void
      isValidationRequired_WhenSerializableIsolationWithGetNotInWriteOrDeleteSet_ShouldReturnTrue() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);

    when(snapshot.isScanSetEmpty()).thenReturn(true);
    when(snapshot.isScannerSetEmpty()).thenReturn(true);
    when(snapshot.getGetSet())
        .thenReturn(
            Collections.singletonList(
                new java.util.AbstractMap.SimpleEntry<>(get, Optional.empty())));
    when(snapshot.containsKeyInWriteSet(key)).thenReturn(false);
    when(snapshot.containsKeyInDeleteSet(key)).thenReturn(false);

    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void
      isValidationRequired_WhenSerializableIsolationWithAllGetsInWriteSet_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);

    when(snapshot.isScanSetEmpty()).thenReturn(true);
    when(snapshot.isScannerSetEmpty()).thenReturn(true);
    when(snapshot.getGetSet())
        .thenReturn(
            Collections.singletonList(
                new java.util.AbstractMap.SimpleEntry<>(get, Optional.empty())));
    when(snapshot.containsKeyInWriteSet(key)).thenReturn(true);

    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void
      isValidationRequired_WhenSerializableIsolationWithAllGetsInDeleteSet_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);

    when(snapshot.isScanSetEmpty()).thenReturn(true);
    when(snapshot.isScannerSetEmpty()).thenReturn(true);
    when(snapshot.getGetSet())
        .thenReturn(
            Collections.singletonList(
                new java.util.AbstractMap.SimpleEntry<>(get, Optional.empty())));
    when(snapshot.containsKeyInWriteSet(key)).thenReturn(false);
    when(snapshot.containsKeyInDeleteSet(key)).thenReturn(true);

    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isValidationRequired_WhenSerializableIsolationWithEmptySets_ShouldReturnFalse() {
    // Arrange
    when(snapshot.isScanSetEmpty()).thenReturn(true);
    when(snapshot.isScannerSetEmpty()).thenReturn(true);
    when(snapshot.getGetSet()).thenReturn(Collections.emptyList());

    TransactionContext context =
        new TransactionContext(ANY_ID, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = context.isValidationRequired();

    // Assert
    assertThat(actual).isFalse();
  }
}
