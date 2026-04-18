package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Put;
import com.scalar.db.io.Key;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConsensusCommitOperationAttributesTest {

  @Test
  public void enableImplicitPreRead_PutGiven_ShouldReturnPutWithImplicitPreReadEnabled() {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("table").partitionKey(Key.ofInt("p", 0)).build();

    // Act
    Put result = ConsensusCommitOperationAttributes.enableImplicitPreRead(put);

    // Assert
    assertThat(result.getAttribute(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED))
        .hasValue("true");
  }

  @Test
  public void enableImplicitPreRead_MapGiven_ShouldAddImplicitPreReadEnabledToAttributes() {
    // Arrange
    java.util.Map<String, String> attributes = new java.util.HashMap<>();

    // Act
    ConsensusCommitOperationAttributes.enableImplicitPreRead(attributes);

    // Assert
    assertThat(attributes)
        .containsEntry(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED, "true");
  }

  @Test
  public void disableImplicitPreRead_PutGiven_ShouldReturnPutWithImplicitPreReadDisabled() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("table")
            .partitionKey(Key.ofInt("p", 0))
            .enableImplicitPreRead()
            .build();

    // Act
    Put result = ConsensusCommitOperationAttributes.disableImplicitPreRead(put);

    // Assert
    assertThat(result.getAttribute(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED))
        .isEmpty();
  }

  @Test
  public void disableImplicitPreRead_MapGiven_ShouldRemoveImplicitPreReadEnabledFromAttributes() {
    // Arrange
    java.util.Map<String, String> attributes = new java.util.HashMap<>();
    attributes.put(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED, "true");

    // Act
    ConsensusCommitOperationAttributes.disableImplicitPreRead(attributes);

    // Assert
    assertThat(attributes)
        .doesNotContainKey(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED);
  }

  @Test
  public void enableInsertMode_PutGiven_ShouldReturnPutWithInsertModeEnabled() {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("table").partitionKey(Key.ofInt("p", 0)).build();

    // Act
    Put result = ConsensusCommitOperationAttributes.enableInsertMode(put);

    // Assert
    assertThat(result.getAttribute(ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED))
        .hasValue("true");
  }

  @Test
  public void enableInsertMode_MapGiven_ShouldAddInsertModeEnabledToAttributes() {
    // Arrange
    java.util.Map<String, String> attributes = new java.util.HashMap<>();

    // Act
    ConsensusCommitOperationAttributes.enableInsertMode(attributes);

    // Assert
    assertThat(attributes)
        .containsEntry(ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED, "true");
  }

  @Test
  public void disableInsertMode_PutGiven_ShouldReturnPutWithInsertModeDisabled() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("table")
            .partitionKey(Key.ofInt("p", 0))
            .enableInsertMode()
            .build();

    // Act
    Put result = ConsensusCommitOperationAttributes.disableInsertMode(put);

    // Assert
    assertThat(result.getAttribute(ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED))
        .isEmpty();
  }

  @Test
  public void disableInsertMode_MapGiven_ShouldRemoveInsertModeEnabledFromAttributes() {
    // Arrange
    java.util.Map<String, String> attributes = new java.util.HashMap<>();
    attributes.put(ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED, "true");

    // Act
    ConsensusCommitOperationAttributes.disableInsertMode(attributes);

    // Assert
    assertThat(attributes)
        .doesNotContainKey(ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED);
  }

  @Test
  public void isImplicitPreReadEnabled_PutWithImplicitPreReadEnabledGiven_ShouldReturnTrue() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("table")
            .partitionKey(Key.ofInt("p", 0))
            .enableImplicitPreRead()
            .build();

    // Act
    boolean result = ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(put);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isImplicitPreReadEnabled_PutWithImplicitPreReadDisabledGiven_ShouldReturnFalse() {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("table").partitionKey(Key.ofInt("p", 0)).build();

    // Act
    boolean result = ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(put);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void isInsertModeEnabled_PutWithInsertModeEnabledGiven_ShouldReturnTrue() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("table")
            .partitionKey(Key.ofInt("p", 0))
            .enableInsertMode()
            .build();

    // Act
    boolean result = ConsensusCommitOperationAttributes.isInsertModeEnabled(put);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isInsertModeEnabled_PutWithInsertModeDisabledGiven_ShouldReturnFalse() {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("table").partitionKey(Key.ofInt("p", 0)).build();

    // Act
    boolean result = ConsensusCommitOperationAttributes.isInsertModeEnabled(put);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void setIsolation_MapGiven_ShouldAddIsolationToAttributes() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    ConsensusCommitOperationAttributes.setIsolation(attributes, Isolation.SERIALIZABLE);

    // Assert
    assertThat(attributes)
        .containsEntry(ConsensusCommitOperationAttributes.ISOLATION, "SERIALIZABLE");
  }

  @Test
  public void clearIsolation_MapGiven_ShouldRemoveIsolationFromAttributes() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsensusCommitOperationAttributes.ISOLATION, "SNAPSHOT");

    // Act
    ConsensusCommitOperationAttributes.clearIsolation(attributes);

    // Assert
    assertThat(attributes).doesNotContainKey(ConsensusCommitOperationAttributes.ISOLATION);
  }

  @Test
  public void getIsolation_IsolationSetInAttributes_ShouldReturnIsolation() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsensusCommitOperationAttributes.ISOLATION, "SERIALIZABLE");

    // Act Assert
    assertThat(ConsensusCommitOperationAttributes.getIsolation(attributes))
        .hasValue(Isolation.SERIALIZABLE);
  }

  @Test
  public void getIsolation_IsolationSetInLowerCaseInAttributes_ShouldReturnIsolation() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsensusCommitOperationAttributes.ISOLATION, "snapshot");

    // Act Assert
    assertThat(ConsensusCommitOperationAttributes.getIsolation(attributes))
        .hasValue(Isolation.SNAPSHOT);
  }

  @Test
  public void getIsolation_IsolationNotSetInAttributes_ShouldReturnEmpty() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act Assert
    assertThat(ConsensusCommitOperationAttributes.getIsolation(attributes)).isEmpty();
  }

  @Test
  public void getIsolation_InvalidIsolationSetInAttributes_ShouldThrowException() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsensusCommitOperationAttributes.ISOLATION, "invalid");

    // Act Assert
    assertThatThrownBy(() -> ConsensusCommitOperationAttributes.getIsolation(attributes))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
